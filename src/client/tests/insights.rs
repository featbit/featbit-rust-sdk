use super::*;
use crate::model::DataSet;
use crate::store::PatchResult;
use serde_json::{json, Value};

fn data(setting: Option<&Value>, version: i64) -> DataSet {
    let mut envelope: Value = serde_json::from_str(READY_BOOTSTRAP).unwrap();
    for flag in envelope["data"]["featureFlags"].as_array_mut().unwrap() {
        flag["updatedAt"] = json!(version);
        if let Some(value) = setting {
            flag["insightsEnabled"] = value.clone();
        }
    }
    serde_json::from_value::<DataSyncEnvelope>(envelope)
        .unwrap()
        .data
}

fn client(url: &str, disable: bool, allow: bool, observer: RecordingObserver) -> FbClient {
    let options = FbOptionsBuilder::new("valid-secret")
        .streaming_url("ws://127.0.0.1:9")
        .event_url(url)
        .start_wait(Duration::from_millis(2))
        .connect_timeout(Duration::from_millis(1))
        .close_timeout(Duration::from_millis(200))
        .auto_flush_interval(Duration::from_mins(1))
        .max_events_per_request(50)
        .disable_events(disable, allow)
        .evaluation_observer(observer)
        .build()
        .unwrap();
    let client = FbClient::with_options(options);
    client.inner.status.set(SyncStatus::Ready);
    client
}

#[test]
fn insights_all_entry_points_respect_global_modes_and_preserve_observers() {
    for setting in [
        None,
        Some(json!(true)),
        Some(json!(false)),
        Some(Value::Null),
        Some(json!(0)),
        Some(json!("false")),
    ] {
        let enabled = setting != Some(json!(false));
        for (disable, allow) in [(false, true), (false, false), (true, true), (true, false)] {
            let mut server = mockito::Server::new();
            let automatic = usize::from(enabled && !disable) * 11;
            let explicit = usize::from(enabled && allow) * 2;
            let count = automatic + explicit + usize::from(allow);
            let request = server.mock("POST", "/api/public/insight/track");
            // Check the actual batch size and that internal eligibility never leaks on the wire.
            let request = request
                .match_request(move |request| {
                    let body: Value = serde_json::from_slice(request.body().unwrap()).unwrap();
                    body.as_array().is_some_and(|events| events.len() == count)
                        && !request
                            .body()
                            .unwrap()
                            .windows(b"insightsEnabled".len())
                            .any(|w| w == b"insightsEnabled")
                })
                .with_status(202)
                .expect(usize::from(count > 0))
                .create();
            let observer = RecordingObserver::default();
            let client = client(&server.url(), disable, allow, observer.clone());
            let mut dataset = data(setting.as_ref(), 1);
            for (key, value) in [("number", "42"), ("json", "{}"), ("string", "hello")] {
                let mut flag = dataset.feature_flags[0].clone();
                flag.key = key.to_owned();
                flag.variations[0].value = value.to_owned();
                dataset.feature_flags.push(flag);
            }
            client.inner.store.populate(&dataset);
            let user = FbUser::builder("insights-user").build();
            assert!(client.bool_variation("enabled", &user, false));
            let detail = client.bool_variation_detail("enabled", &user, false);
            assert!(detail.value);
            assert_eq!(client.int_variation("number", &user, 0), 42);
            assert_eq!(client.int_variation_detail("number", &user, 0).value, 42);
            assert!((client.float_variation("number", &user, 0.0) - 42.0).abs() < f64::EPSILON);
            assert!(
                (client.float_variation_detail("number", &user, 0.0).value - 42.0).abs()
                    < f64::EPSILON
            );
            assert_eq!(client.string_variation("string", &user, ""), "hello");
            assert_eq!(
                client.string_variation_detail("string", &user, "").value,
                "hello"
            );
            assert_eq!(client.json_variation("json", &user, Value::Null), json!({}));
            assert_eq!(
                client
                    .json_variation_detail("json", &user, Value::Null)
                    .value,
                json!({})
            );
            let raw = client.evaluate_raw("enabled", &user).unwrap();
            client.complete_raw_evaluation(&user, &raw);
            let event = detail.evaluation_event.unwrap();
            assert_eq!(event.insights_enabled(), enabled);
            assert_eq!(client.track_eval_event(&user, &event), enabled && allow);
            assert_eq!(
                client.track_eval_event_for_flag(&user, "enabled"),
                enabled && allow
            );
            assert_eq!(client.track_metric_event(&user, "purchase", 1.0), allow);
            assert!(client
                .all_variations(&user)
                .iter()
                .all(|d| d.evaluation_event.is_none()));
            assert!(!client.bool_variation("invalid-bool", &user, false));
            assert_eq!(client.int_variation_detail("string", &user, -1).value, -1);
            assert!(client
                .json_variation("string", &user, Value::Null)
                .is_null());
            assert_eq!(observer.observations.lock().unwrap().len(), 14);
            assert!(client.flush_and_wait(Duration::from_secs(2)));
            client.close();
            assert!(!client.track_eval_event(&user, &event));
            request.assert();
        }
    }
}

#[test]
fn insights_captured_detail_and_raw_survive_patch_in_both_directions() {
    for disable in [false, true] {
        let (url, bodies, server) = scripted_http_server([202]);
        let client = client(&url, disable, true, RecordingObserver::default());
        client.inner.store.populate(&data(Some(&json!(true)), 1));
        let user = FbUser::builder("user").build();
        let enabled = client.bool_variation_detail("enabled", &user, false);
        let raw_enabled = client.evaluate_raw("enabled", &user).unwrap();
        assert_eq!(
            client.inner.store.patch(&data(Some(&json!(false)), 2)),
            PatchResult::Changed
        );
        let disabled = client.bool_variation_detail("enabled", &user, false);
        let raw_disabled = client.evaluate_raw("enabled", &user).unwrap();
        assert_eq!(enabled, disabled);
        assert!(client.track_eval_event(&user, enabled.evaluation_event.as_ref().unwrap()));
        client.complete_raw_evaluation(&user, &raw_enabled);
        assert_eq!(
            client.inner.store.patch(&data(Some(&json!(true)), 3)),
            PatchResult::Changed
        );
        assert!(!client.track_eval_event(&user, disabled.evaluation_event.as_ref().unwrap()));
        client.complete_raw_evaluation(&user, &raw_disabled);
        assert!(client.track_eval_event_for_flag(&user, "enabled"));
        assert!(client.flush_and_wait(Duration::from_secs(2)));
        client.close();
        server.join().unwrap();
        let body: Value =
            serde_json::from_slice(&bodies.recv_timeout(Duration::from_secs(1)).unwrap()).unwrap();
        assert_eq!(body.as_array().unwrap().len(), if disable { 2 } else { 4 });
    }
}

#[test]
fn insights_offline_bootstrap_retains_eligibility_but_never_tracks() {
    for enabled in [false, true] {
        let bootstrap = READY_BOOTSTRAP.replace(
            "\"isEnabled\":true",
            &format!("\"insightsEnabled\":{enabled},\"isEnabled\":true"),
        );
        let options = FbOptionsBuilder::new("offline-placeholder")
            .offline(true)
            .bootstrap_json(bootstrap)
            .build()
            .unwrap();
        let client = FbClient::with_options(options);
        let user = FbUser::builder("user").build();
        let detail = client.bool_variation_detail("enabled", &user, false);
        assert!(detail.value);
        let event = detail.evaluation_event.unwrap();
        assert_eq!(event.insights_enabled(), enabled);
        assert!(!client.track_eval_event(&user, &event));
        assert!(!client.track_eval_event_for_flag(&user, "enabled"));
        assert!(matches!(
            client.inner.event_processor,
            EventProcessor::Disabled
        ));
        client.close();
    }
}
