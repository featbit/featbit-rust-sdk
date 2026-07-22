use std::sync::Mutex;
use std::thread;

use super::*;
use crate::model::DataSyncEnvelope;
use crate::observation::{
    EvaluationObservation, EvaluationObservationError, EvaluationObservationReason,
    EvaluationObserver,
};
use crate::test_support::scripted_http_server;

#[derive(Clone, Default)]
struct RecordingObserver {
    observations: Arc<Mutex<Vec<EvaluationObservation>>>,
}

impl EvaluationObserver for RecordingObserver {
    fn on_evaluation(&self, observation: &EvaluationObservation) {
        self.observations
            .lock()
            .expect("test observer lock should remain available")
            .push(observation.clone());
    }
}

const READY_BOOTSTRAP: &str = r#"{
        "messageType":"data-sync",
        "data":{"eventType":"full","featureFlags":[{
            "id":"flag-id","key":"enabled","updatedAt":1,"variationType":"boolean",
            "variations":[{"id":"value","value":"true"}],
            "targetUsers":[],"rules":[],"isEnabled":true,
            "fallthrough":{"includedInExpt":false,"variations":[
                {"id":"value","rollout":[0,1],"exptRollout":0}
            ]}
        },{
            "id":"invalid-flag-id","key":"invalid-bool","updatedAt":1,"variationType":"boolean",
            "variations":[{"id":"invalid-value","value":"not-a-boolean"}],
            "targetUsers":[],"rules":[],"isEnabled":true,
            "fallthrough":{"includedInExpt":false,"variations":[
                {"id":"invalid-value","rollout":[0,1],"exptRollout":0}
            ]}
        }],"segments":[]}
    }"#;

const UPDATED_BOOTSTRAP: &str = r#"{
        "messageType":"data-sync",
        "data":{"eventType":"full","featureFlags":[{
            "id":"flag-id","key":"enabled","updatedAt":2,"variationType":"boolean",
            "variations":[{"id":"value","value":"false"}],
            "targetUsers":[],"rules":[],"isEnabled":true,
            "fallthrough":{"includedInExpt":false,"variations":[
                {"id":"value","rollout":[0,1],"exptRollout":0}
            ]}
        }],"segments":[]}
    }"#;

const ALL_VARIATIONS_BOOTSTRAP: &str = r#"{
    "messageType":"data-sync",
    "data":{"eventType":"full","featureFlags":[{
        "id":"b-id","key":"b-flag","updatedAt":1,"variationType":"string",
        "variations":[{"id":"b-value","value":"bravo"}],
        "isEnabled":true,"fallthrough":{"variations":[{"id":"b-value","rollout":[0,1]}]}
    },{
        "id":"a-id","key":"a-flag","updatedAt":1,"variationType":"string",
        "variations":[{"id":"a-value","value":"alpha"}],
        "isEnabled":true,"fallthrough":{"variations":[{"id":"a-value","rollout":[0,1]}]}
    },{
        "id":"archived-id","key":"archived","updatedAt":1,"variationType":"string",
        "variations":[{"id":"archived-value","value":"hidden"}],
        "isEnabled":true,"isArchived":true,
        "fallthrough":{"variations":[{"id":"archived-value","rollout":[0,1]}]}
    },{
        "id":"malformed-id","key":"malformed","updatedAt":1,"variationType":"string",
        "variations":[{"id":"value","value":"unused"}],
        "isEnabled":false,"disabledVariationId":"missing"
    }],"segments":[]}}
"#;

#[test]
fn public_client_is_send_and_sync() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<FbClient>();
}

#[test]
fn uninitialized_client_returns_fallback_without_panicking() {
    let options = FbOptionsBuilder::new("valid-secret")
        .streaming_url("ws://127.0.0.1:9")
        .start_wait(Duration::from_millis(2))
        .connect_timeout(Duration::from_millis(1))
        .build()
        .expect("options should build");
    let client = FbClient::with_options(options);
    let user = FbUser::builder("user").build();
    let detail = client.bool_variation_detail("missing", &user, true);
    assert!(detail.value);
    assert_eq!(detail.kind, ReasonKind::ClientNotReady);
    client.close();
}

#[test]
fn evaluation_and_idempotent_close_are_thread_safe() {
    let options = FbOptionsBuilder::new("valid-secret")
        .offline(true)
        .disable_events(true, false)
        .bootstrap_json(READY_BOOTSTRAP)
        .build()
        .expect("offline options should build");
    let client = FbClient::with_options(options);
    let updated = serde_json::from_str::<DataSyncEnvelope>(UPDATED_BOOTSTRAP)
        .expect("updated data should parse")
        .data;

    let writer_store = Arc::clone(&client.inner.store);
    let writer = thread::spawn(move || {
        for _ in 0..1_000 {
            writer_store.populate(&updated);
        }
    });
    let readers = (0..4)
        .map(|index| {
            let reader = client.clone();
            thread::spawn(move || {
                let user = FbUser::builder(format!("user-{index}")).build();
                for _ in 0..1_000 {
                    let detail = reader.bool_variation_detail("enabled", &user, false);
                    assert_eq!(detail.kind, ReasonKind::Fallthrough);
                    assert_eq!(detail.variation_id, "value");
                }
            })
        })
        .collect::<Vec<_>>();

    writer.join().expect("writer should finish");
    for reader in readers {
        reader.join().expect("reader should finish");
    }

    let closers = (0..4)
        .map(|_| {
            let closer = client.clone();
            thread::spawn(move || closer.close())
        })
        .collect::<Vec<_>>();
    for closer in closers {
        closer.join().expect("close should finish");
    }
    assert_eq!(client.status(), ClientStatus::Closed);
}

#[test]
fn terminal_sync_status_never_evaluates_a_previously_ready_snapshot() {
    let options = FbOptionsBuilder::new("valid-secret")
        .offline(true)
        .disable_events(true, false)
        .bootstrap_json(READY_BOOTSTRAP)
        .build()
        .expect("offline options should build");
    let client = FbClient::with_options(options);
    let user = FbUser::builder("user").build();
    assert!(client.bool_variation("enabled", &user, false));
    assert!(client.initialized());

    client.inner.status.set(SyncStatus::Closed);

    let detail = client.bool_variation_detail("enabled", &user, false);
    assert!(!detail.value);
    assert_eq!(detail.kind, ReasonKind::ClientNotReady);
    assert_eq!(client.status(), ClientStatus::Closed);
    assert!(client.initialized(), "initialized records prior readiness");
    assert!(client.all_variations(&user).is_empty());
    client.close();
}

#[test]
fn all_variations_returns_sorted_successes_without_recording_events() {
    let mut server = mockito::Server::new();
    let no_event_request = server
        .mock("POST", "/api/public/insight/track")
        .expect(0)
        .create();
    let options = FbOptionsBuilder::new("valid-secret")
        .streaming_url("ws://127.0.0.1:9")
        .event_url(server.url())
        .start_wait(Duration::from_millis(2))
        .connect_timeout(Duration::from_millis(1))
        .auto_flush_interval(Duration::from_mins(1))
        .build()
        .expect("online options should build");
    let client = FbClient::with_options(options);
    let data = serde_json::from_str::<DataSyncEnvelope>(ALL_VARIATIONS_BOOTSTRAP)
        .expect("all-variations bootstrap should parse")
        .data;
    client.inner.store.populate(&data);
    client.inner.status.set(SyncStatus::Ready);

    let results = client.all_variations(&FbUser::builder("user-1").build());
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].key, "a-flag");
    assert_eq!(results[0].value, "alpha");
    assert_eq!(results[0].variation_id, "a-value");
    assert_eq!(results[0].kind, ReasonKind::Fallthrough);
    assert_eq!(results[0].reason, "fall through targets and rules");
    assert!(results[0].evaluation_event.is_none());
    assert_eq!(results[1].key, "b-flag");
    assert_eq!(results[1].value, "bravo");
    assert_eq!(results[1].variation_id, "b-value");
    assert!(results[1].evaluation_event.is_none());
    assert!(results.iter().all(|result| result.key != "archived"));
    assert!(results.iter().all(|result| result.key != "malformed"));

    assert!(client.flush_and_wait(Duration::from_secs(1)));
    client.close();
    no_event_request.assert();
}

#[test]
fn retained_evaluation_event_survives_a_newer_flag_snapshot() {
    let (event_url, bodies, server) = scripted_http_server([202]);
    let options = FbOptionsBuilder::new("valid-secret")
        .streaming_url("ws://127.0.0.1:9")
        .event_url(event_url)
        .start_wait(Duration::from_millis(2))
        .connect_timeout(Duration::from_millis(1))
        .auto_flush_interval(Duration::from_mins(1))
        .disable_events(true, true)
        .build()
        .expect("online options should build");
    let client = FbClient::with_options(options);
    let initial = serde_json::from_str::<DataSyncEnvelope>(READY_BOOTSTRAP)
        .expect("initial data should parse")
        .data;
    client.inner.store.populate(&initial);
    client.inner.status.set(SyncStatus::Ready);

    let user = FbUser::builder("user-1").build();
    let detail = client.bool_variation_detail("enabled", &user, false);
    assert!(detail.value);
    let retained = detail
        .evaluation_event
        .expect("successful detail should retain an event");

    let updated = serde_json::from_str::<DataSyncEnvelope>(UPDATED_BOOTSTRAP)
        .expect("updated data should parse")
        .data;
    client.inner.store.populate(&updated);
    assert!(!client.bool_variation("enabled", &user, true));

    assert!(client.track_eval_event(&user, &retained));
    assert!(client.flush_and_wait(Duration::from_secs(2)));
    client.close();
    server.join().expect("event server should stop");

    let body = bodies
        .recv_timeout(Duration::from_secs(1))
        .expect("retained event should be delivered");
    let batch: serde_json::Value =
        serde_json::from_slice(&body).expect("event batch should be JSON");
    let variation = &batch[0]["variations"][0]["variation"];
    assert_eq!(variation["id"], "value");
    assert_eq!(variation["value"], "true");
}

#[test]
fn event_modes_control_automatic_and_explicit_delivery() {
    for (disable, allow_track, expected_evaluations, expected_metrics) in [
        (false, true, 2, 1),
        (false, false, 1, 0),
        (true, true, 1, 1),
        (true, false, 0, 0),
    ] {
        let expected_total = expected_evaluations + expected_metrics;
        let statuses = if expected_total == 0 {
            Vec::new()
        } else {
            vec![202]
        };
        let (event_url, bodies, server) = scripted_http_server(statuses);
        let options = FbOptionsBuilder::new("valid-secret")
            .streaming_url("ws://127.0.0.1:9")
            .event_url(event_url)
            .start_wait(Duration::from_millis(2))
            .connect_timeout(Duration::from_millis(1))
            .close_timeout(Duration::from_millis(200))
            .auto_flush_interval(Duration::from_mins(1))
            .flush_timeout(Duration::from_secs(2))
            .disable_events(disable, allow_track)
            .build()
            .expect("online options should build");
        let client = FbClient::with_options(options);
        let data = serde_json::from_str::<DataSyncEnvelope>(READY_BOOTSTRAP)
            .expect("bootstrap should parse")
            .data;
        client.inner.store.populate(&data);
        client.inner.status.set(SyncStatus::Ready);

        let user = FbUser::builder("user").build();
        let detail = client.bool_variation_detail("enabled", &user, false);
        let evaluation_event = detail
            .evaluation_event
            .as_ref()
            .expect("successful detail should retain its evaluation event");
        assert_eq!(
            client.track_eval_event(&user, evaluation_event),
            allow_track
        );
        assert_eq!(
            client.track_metric_event(&user, "converted", 1.0),
            allow_track
        );
        assert_eq!(
            matches!(&client.inner.event_processor, EventProcessor::Disabled),
            disable && !allow_track
        );
        assert!(client.flush_and_wait(Duration::from_secs(2)));
        client.close();
        server.join().expect("event server should stop");

        if expected_total == 0 {
            assert!(bodies.try_recv().is_err());
            continue;
        }
        let body = bodies
            .recv_timeout(Duration::from_secs(1))
            .expect("configured events should be delivered");
        let events =
            serde_json::from_slice::<serde_json::Value>(&body).expect("event batch should be JSON");
        let events = events.as_array().expect("event batch should be an array");
        assert_eq!(events.len(), expected_total);
        assert_eq!(
            events
                .iter()
                .filter(|event| event.get("variations").is_some())
                .count(),
            expected_evaluations
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.get("metrics").is_some())
                .count(),
            expected_metrics
        );
    }
}

#[test]
fn observer_is_independent_from_featbit_event_delivery() {
    let observer = RecordingObserver::default();
    let observations = Arc::clone(&observer.observations);
    let options = FbOptionsBuilder::new("valid-secret")
        .offline(true)
        .disable_events(true, false)
        .evaluation_observer(observer)
        .bootstrap_json(READY_BOOTSTRAP)
        .build()
        .expect("offline options should build");
    let client = FbClient::with_options(options);
    let user = FbUser::builder("private-user-key").build();

    let detail = client.bool_variation_detail("enabled", &user, false);
    assert!(detail.value);
    assert!(!client.track_metric_event(&user, "disabled", 1.0));
    assert!(!client.track_eval_event(
        &user,
        detail
            .evaluation_event
            .as_ref()
            .expect("successful evaluation should retain an event")
    ));
    let _fallback = client.bool_variation("missing", &user, false);
    let mismatch = client.bool_variation_detail("invalid-bool", &user, true);
    assert!(mismatch.value);
    assert_eq!(mismatch.kind, ReasonKind::WrongType);
    assert!(mismatch.evaluation_event.is_none());

    let observations = observations
        .lock()
        .expect("test observer lock should remain available");
    assert_eq!(observations.len(), 3);
    assert_eq!(
        observations[0].reason(),
        EvaluationObservationReason::Default
    );
    assert_eq!(observations[0].variation_id(), Some("value"));
    assert_eq!(
        observations[1].error_type(),
        Some(EvaluationObservationError::FlagNotFound)
    );
    assert_eq!(
        observations[2].error_type(),
        Some(EvaluationObservationError::TypeMismatch)
    );
    assert!(!format!("{:?}", observations[0]).contains("private-user-key"));
    client.close();
}
