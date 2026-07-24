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
