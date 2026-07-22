//! Evaluates one boolean flag through the native `FbClient` API in a console application.

use std::env;
use std::error::Error;

use featbit_server_sdk::{FbClient, FbOptions, FbUser};

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    env_logger::init();

    let (Ok(secret), Ok(streaming_url), Ok(event_url)) = (
        env::var("FEATBIT_ENV_SECRET"),
        env::var("FEATBIT_STREAMING_URL"),
        env::var("FEATBIT_EVENT_URL"),
    ) else {
        eprintln!(
            "set FEATBIT_ENV_SECRET, FEATBIT_STREAMING_URL, and FEATBIT_EVENT_URL to run this example"
        );
        return Ok(());
    };

    let options = FbOptions::builder(secret)
        .streaming_url(streaming_url)
        .event_url(event_url)
        .build()?;
    let client = FbClient::with_options(options);
    let user = FbUser::builder("example-user")
        .name("Example User")
        .custom("country", "CN")
        .build();

    let detail = client.bool_variation_detail("example-flag", &user, false);
    let variation_id = if detail.variation_id.is_empty() {
        "<fallback>"
    } else {
        &detail.variation_id
    };
    println!(
        "{}: {} (variation: {variation_id}, reason: {})",
        detail.key, detail.value, detail.reason
    );

    client.close();
    Ok(())
}
