//! Registers the `FeatBit` provider and evaluates one boolean flag through `OpenFeature` in a
//! console application.

use std::env;
use std::error::Error;

use featbit_server_sdk::{FbOptions, FeatBitProvider};
use open_feature::{EvaluationContext, OpenFeature};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
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
    let provider = tokio::task::spawn_blocking(move || FeatBitProvider::new(options)).await?;
    let featbit = provider.client().clone();

    let client = {
        let mut api = OpenFeature::singleton_mut().await;
        api.set_provider(provider).await;
        api.create_client()
    };
    let context = EvaluationContext::default()
        .with_targeting_key("example-user")
        .with_custom_field("name", "Example User")
        .with_custom_field("country", "CN");
    let enabled = client
        .get_bool_value("example-flag", Some(&context), None)
        .await
        .unwrap_or(false);
    println!("example-flag: {enabled}");

    OpenFeature::singleton_mut().await.shutdown().await;
    tokio::task::spawn_blocking(move || featbit.close()).await?;
    Ok(())
}
