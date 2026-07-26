use std::sync::Arc;

use slack_morphism::errors::{SlackClientEndOfStreamError, SlackClientSocketModeProtocolError};

use super::*;
use crate::test_support::{FakeSlackClient, RecordingBenchmarkQueue};

const APP_MENTION_JSON: &str = r#"{
    "team_id": "T_OK",
    "api_app_id": "A123",
    "event": {
        "type": "app_mention",
        "user": "U_OK",
        "channel": "C1",
        "text": "<@U07BOT> bench --block 184231",
        "ts": "1700000000.000100"
    },
    "event_id": "Ev123",
    "event_time": 1700000000
}"#;

fn parse_callback() -> SlackPushEventCallback {
    serde_json::from_str(APP_MENTION_JSON).expect("valid app_mention envelope")
}

fn config() -> SlackConnectorConfig {
    SlackConnectorConfig::new("develop", vec!["T_OK".into()], vec!["U_OK".into()])
}

#[test]
fn maps_app_mention_to_provider_neutral_event() {
    let mention = mention_from_callback(&parse_callback()).expect("app mention maps");
    assert_eq!(mention.team_id, "T_OK");
    assert_eq!(mention.user, "U_OK");
    assert_eq!(mention.channel, "C1");
    assert_eq!(mention.message_ts, "1700000000.000100");
}

#[tokio::test]
async fn dispatched_mention_enqueues_off_the_ack_path() {
    let queue = Arc::new(RecordingBenchmarkQueue::default());
    let client = Arc::new(FakeSlackClient::default());
    let connector = Arc::new(SlackConnector::new(
        config(),
        SlackJobTarget {
            installation_id: 100,
            repo_id: 10,
        },
        queue.clone(),
        client,
    ));
    spawn_dispatch(Some(connector), mention_from_callback(&parse_callback()).unwrap());
    for _ in 0..1000 {
        if !queue.jobs().is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(queue.jobs().len(), 1);
}

#[test]
fn only_socket_transport_resets_are_transient() {
    let reset: Box<dyn std::error::Error + Send + Sync + 'static> = Box::new(
        SlackClientError::SocketModeProtocolError(SlackClientSocketModeProtocolError::new(
            "Slack WSS error: Protocol(ResetWithoutClosingHandshake)".into(),
        )),
    );
    assert!(is_transient_socket_error(&*reset));

    let end: Box<dyn std::error::Error + Send + Sync + 'static> =
        Box::new(SlackClientError::EndOfStream(SlackClientEndOfStreamError::new()));
    assert!(!is_transient_socket_error(&*end));
}
