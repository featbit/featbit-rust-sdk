//! Standalone microbenchmark for the direct and detail evaluation hot paths.

use std::hint::black_box;
use std::time::Instant;

use featbit_server_sdk::{FbClient, FbOptions, FbUser};

const READY_BOOTSTRAP: &str = r#"{
    "messageType":"data-sync",
    "data":{"eventType":"full","featureFlags":[{
        "id":"flag-id","key":"enabled","updatedAt":1,"variationType":"boolean",
        "variations":[{"id":"on","value":"true"}],
        "targetUsers":[],"rules":[],"isEnabled":true,
        "fallthrough":{"includedInExpt":false,"variations":[
            {"id":"on","rollout":[0,1],"exptRollout":0}
        ]}
    },{
        "id":"rollout-id","key":"test-","updatedAt":1,"variationType":"string",
        "variations":[
            {"id":"v0","value":"0"},{"id":"v1","value":"1"},
            {"id":"v2","value":"2"},{"id":"v3","value":"3"},
            {"id":"v4","value":"4"},{"id":"v5","value":"5"},
            {"id":"v6","value":"6"},{"id":"v7","value":"7"},
            {"id":"v8","value":"8"},{"id":"v9","value":"9"}
        ],
        "targetUsers":[],"rules":[],"isEnabled":true,
        "fallthrough":{"includedInExpt":false,"variations":[
            {"id":"v0","rollout":[0.0,0.1],"exptRollout":0},
            {"id":"v1","rollout":[0.1,0.2],"exptRollout":0},
            {"id":"v2","rollout":[0.2,0.3],"exptRollout":0},
            {"id":"v3","rollout":[0.3,0.4],"exptRollout":0},
            {"id":"v4","rollout":[0.4,0.5],"exptRollout":0},
            {"id":"v5","rollout":[0.5,0.6],"exptRollout":0},
            {"id":"v6","rollout":[0.6,0.7],"exptRollout":0},
            {"id":"v7","rollout":[0.7,0.8],"exptRollout":0},
            {"id":"v8","rollout":[0.8,0.9],"exptRollout":0},
            {"id":"v9","rollout":[0.9,1.0],"exptRollout":0}
        ]}
    }],"segments":[]}
}"#;

fn main() {
    let Ok(options) = FbOptions::builder("benchmark-secret")
        .offline(true)
        .disable_events(true, false)
        .bootstrap_json(READY_BOOTSTRAP)
        .build()
    else {
        eprintln!("failed to build evaluation benchmark options");
        return;
    };
    let client = FbClient::with_options(options);
    let user = FbUser::builder("benchmark-user").build();
    let rollout_user = FbUser::builder("user-default").build();
    if client.string_variation("test-", &rollout_user, "fallback") != "5" {
        eprintln!("failed to resolve the ten-way rollout benchmark fixture");
        client.close();
        return;
    }

    run("bool_variation", 2_000_000, || {
        black_box(client.bool_variation(black_box("enabled"), black_box(&user), black_box(false)));
    });
    run("bool_variation_detail", 200_000, || {
        black_box(client.bool_variation_detail(
            black_box("enabled"),
            black_box(&user),
            black_box(false),
        ));
    });
    run("ten_way_rollout", 500_000, || {
        black_box(client.string_variation(
            black_box("test-"),
            black_box(&rollout_user),
            black_box("fallback"),
        ));
    });

    client.close();
}

fn run(name: &str, iterations: u64, mut evaluate: impl FnMut()) {
    for _ in 0..10_000 {
        evaluate();
    }

    let started = Instant::now();
    for _ in 0..iterations {
        evaluate();
    }
    let elapsed = started.elapsed();
    let nanos_per_evaluation = elapsed.as_nanos() / u128::from(iterations);
    println!("{name}: {nanos_per_evaluation} ns/evaluation ({iterations} iterations)");
}
