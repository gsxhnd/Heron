//! 端到端示例：连接 broker、订阅主题、发布一条消息并等待回环投递。
//!
//! ```text
//! MQTT_BROKER=127.0.0.1:1883 MQTT_PROTOCOL=v5 cargo run -p mqtt_client --example pubsub
//! ```

use std::time::Duration;

use mqtt_client::{ClientConfig, ClientError, Event, MqttClient, ProtocolVersion};
use tokio::sync::mpsc;

const TOPIC: &str = "heron/example";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> Result<(), ClientError> {
    let broker = std::env::var("MQTT_BROKER").unwrap_or_else(|_| "127.0.0.1:1883".to_string());
    let protocol = match std::env::var("MQTT_PROTOCOL").as_deref() {
        Ok("v5") | Ok("5") => ProtocolVersion::V5,
        _ => ProtocolVersion::V4,
    };

    let config = ClientConfig::new(&broker, "mqtt-client-example")
        .protocol_version(protocol)
        .request_timeout(REQUEST_TIMEOUT);

    println!("connecting to {broker} ({})", protocol.label());
    let (client, mut events) = MqttClient::new(config);

    loop {
        match events.recv().await {
            Some(Event::Connected { session_present }) => {
                println!("connected (session_present={session_present})");
                break;
            }
            Some(Event::Disconnected { reason }) => {
                eprintln!("connect failed: {}", reason.unwrap_or_default());
                return Err(ClientError::Disconnected);
            }
            Some(_) => continue,
            None => return Err(ClientError::Disconnected),
        }
    }

    // subscribe 直接返回 broker 授权的 QoS / reason code。
    let granted = client.subscribe(vec![(TOPIC.to_string(), 1)]).await?;
    println!("subscribed to {TOPIC}, granted {granted:?}");

    // QoS 1 发布会一直等到 PUBACK。
    client
        .publish(TOPIC, "hello from mqtt_client", 1, false)
        .await?;
    println!("published (PUBACK received)");

    match tokio::time::timeout(REQUEST_TIMEOUT, next_message(&mut events)).await {
        Ok(Some((topic, payload, qos))) => {
            println!(
                "received on {topic} (qos {qos}): {}",
                String::from_utf8_lossy(&payload)
            );
        }
        Ok(None) => eprintln!("event channel closed"),
        Err(_) => eprintln!("no message within {REQUEST_TIMEOUT:?}"),
    }

    client.unsubscribe(vec![TOPIC.to_string()]).await?;
    client.disconnect().await.ok();
    Ok(())
}

async fn next_message(events: &mut mpsc::Receiver<Event>) -> Option<(String, Vec<u8>, u8)> {
    loop {
        match events.recv().await {
            Some(Event::Message {
                topic,
                payload,
                qos,
                ..
            }) => return Some((topic, payload.to_vec(), qos)),
            Some(_) => continue,
            None => return None,
        }
    }
}
