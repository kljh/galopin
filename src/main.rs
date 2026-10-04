//! Subscribe to an AWS IoT Core MQTT topic and republish each message
//! (wrapped with some metadata) to another topic.
//!
//! Configuration via environment variables:
//!   AWS_IOT_ENDPOINT   e.g. abc123xyz-ats.iot.eu-west-2.amazonaws.com   (required)
//!   AWS_IOT_CLIENT_ID  MQTT client id (must be allowed by your IoT policy)  [hands-free-phone-iot]
//!   SUB_TOPIC          topic (wildcards OK) to subscribe to                 [devices/+/outgoing-call]
//!   PUB_TOPIC          topic to publish to                                  [devices/incoming-call]
//!   CA_PATH            Amazon Root CA                                       [certs/AmazonRootCA1.pem]
//!   CERT_PATH          device certificate                                   [certs/device.pem.crt]
//!   KEY_PATH           device private key                                   [certs/device-private.pem.key]
//!
//! With `--publish <MESSAGE>`, publishes MESSAGE once to PUB_TOPIC and exits
//! instead of relaying.

use std::{
    env, fs,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use clap::Parser;
use rumqttc::{
    AsyncClient, Event, EventLoop, MqttOptions, Outgoing, Packet, QoS, TlsConfiguration, Transport,
};
use serde_json::{json, Value};
use tokio::sync::mpsc;

#[derive(Parser)]
#[command(about = "Relay AWS IoT Core MQTT messages from SUB_TOPIC to PUB_TOPIC")]
struct Args {
    /// Publish MESSAGE once to PUB_TOPIC and exit instead of relaying.
    #[arg(long, value_name = "MESSAGE")]
    publish: Option<String>,
}

struct Config {
    endpoint: String,
    client_id: String,
    sub_topic: String,
    pub_topic: String,
    ca_path: String,
    cert_path: String,
    key_path: String,
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

impl Config {
    fn from_env() -> Result<Self> {
        Ok(Self {
            // aws iot describe-endpoint --endpoint-type iot:Data-ATS
            endpoint: env::var("AWS_IOT_ENDPOINT").context("AWS_IOT_ENDPOINT must be set")?,
            ca_path: env_or("CA_PATH", "certs/AmazonRootCA1.pem"),

            client_id: env_or("AWS_IOT_CLIENT_ID", "hands-free-phone-iot"),
            cert_path: env_or("CERT_PATH", "certs/device.pem.crt"),
            key_path: env_or("KEY_PATH", "certs/device-private.pem.key"),
            // For testing with the AWS IoT test certificate, use:
            // client_id: env_or("AWS_IOT_CLIENT_ID", "hands-free-phone-iot-test"),
            // cert_path: env_or("CERT_PATH", "certs/test.pem.crt"),
            // key_path: env_or("KEY_PATH", "certs/test-private.pem.key"),
            
            sub_topic: env_or("SUB_TOPIC", "devices/outgoing-call"),
            pub_topic: env_or("PUB_TOPIC", "devices/incoming-call"),
        })
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Build the outgoing payload from an incoming message.
/// Replace this with whatever processing you need.
fn transform(source_topic: &str, payload: &[u8]) -> Vec<u8> {
    // Keep JSON as JSON; otherwise pass it through as a string.
    let body: Value = serde_json::from_slice(payload)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(payload).into_owned()));

    json!({
        "source_topic": source_topic,
        "received_at_ms": now_ms(),
        "payload": body,
    })
    .to_string()
    .into_bytes()
}

fn mqtt_client(cfg: &Config) -> Result<(AsyncClient, EventLoop)> {
    // --- TLS (mutual auth with the device certificate) ---
    let ca = fs::read(&cfg.ca_path).with_context(|| format!("reading {}", cfg.ca_path))?;
    let cert = fs::read(&cfg.cert_path).with_context(|| format!("reading {}", cfg.cert_path))?;
    let key = fs::read(&cfg.key_path).with_context(|| format!("reading {}", cfg.key_path))?;

    let mut opts = MqttOptions::new(&cfg.client_id, &cfg.endpoint, 8883);
    opts.set_keep_alive(Duration::from_secs(30));
    opts.set_transport(Transport::Tls(TlsConfiguration::Simple {
        ca,
        alpn: None,
        client_auth: Some((cert, key)),
    }));

    Ok(AsyncClient::new(opts, 100))
}

/// Publish a single message, wait for the broker's PubAck, then disconnect.
async fn publish_once(cfg: &Config, message: String) -> Result<()> {
    let (client, mut eventloop) = mqtt_client(cfg)?;
    println!("connecting to {} as '{}'", cfg.endpoint, cfg.client_id);

    // Queued now; the event loop sends it once connected.
    // Either pub_topic (incoming_call) or sub_topic (outgoing_call) depending on what we want to simulate.
    let topic = &cfg.sub_topic;
    client
        .publish(topic, QoS::AtLeastOnce, false, message.into_bytes())
        .await?;

    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let event = eventloop.poll().await.context("MQTT connection failed")?;
            if let Event::Incoming(Packet::PubAck(_)) = event {
                break;
            }
        }
        println!("published to {}", topic);

        client.disconnect().await?;
        loop {
            match eventloop.poll().await {
                Ok(Event::Outgoing(Outgoing::Disconnect)) | Err(_) => break,
                Ok(_) => {}
            }
        }
        anyhow::Ok(())
    })
    .await
    .context("timed out waiting for the broker")?
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cfg = Config::from_env()?;

    if let Some(message) = args.publish {
        return publish_once(&cfg, message).await;
    }

    // Guard against an infinite loop where we receive our own output.
    if cfg.sub_topic == cfg.pub_topic {
        bail!("SUB_TOPIC and PUB_TOPIC must differ, or the relay will loop forever");
    }

    let (client, mut eventloop) = mqtt_client(&cfg)?;

    // Incoming messages are handed to a separate task so the event loop is
    // never blocked waiting on a publish (which would deadlock).
    let (tx, mut rx) = mpsc::channel::<(String, Vec<u8>)>(100);

    let pub_client = client.clone();
    let pub_topic = cfg.pub_topic.clone();
    let worker = tokio::spawn(async move {
        while let Some((topic, payload)) = rx.recv().await {
            let out = transform(&topic, &payload);
            match pub_client
                .publish(&pub_topic, QoS::AtLeastOnce, false, out)
                .await
            {
                Ok(()) => println!("relayed {topic} -> {pub_topic}"),
                Err(e) => eprintln!("publish failed: {e}"),
            }
        }
    });

    println!("connecting to {} as '{}'", cfg.endpoint, cfg.client_id);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("shutting down");
                let _ = client.disconnect().await;
                break;
            }
            event = eventloop.poll() => match event {
                // (Re)subscribe on every successful connect, since we use a clean session.
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    println!("connected; subscribing to {}", cfg.sub_topic);
                    if let Err(e) = client.try_subscribe(&cfg.sub_topic, QoS::AtLeastOnce) {
                        eprintln!("subscribe failed: {e}");
                    }
                }
                Ok(Event::Incoming(Packet::SubAck(ack))) => {
                    println!("subscription ack: {:?}", ack.return_codes);
                }
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    if tx.send((p.topic.clone(), p.payload.to_vec())).await.is_err() {
                        eprintln!("worker stopped; exiting");
                        break;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    // rumqttc reconnects on the next poll; back off a little first.
                    eprintln!("connection error: {e}; retrying in 5s");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    drop(tx);
    let _ = worker.await;
    Ok(())
}