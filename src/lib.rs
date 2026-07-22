//! `FeatBit`'s server-side SDK and `OpenFeature` provider for Rust.
//!
//! The SDK keeps flag data synchronized in the background and evaluates flags locally. Create one
//! [`FbClient`] or [`FeatBitProvider`] per `FeatBit` environment and reuse it for the lifetime of the
//! application.

#![forbid(unsafe_code)]

mod client;
mod data_sync;
mod error;
mod evaluation;
mod events;
mod model;
mod open_feature;
mod options;
mod prepared;
mod store;
mod worker;

pub use client::{ClientStatus, EvaluationDetail, FbClient, ReasonKind};
pub use error::ConfigError;
pub use model::{FbUser, FbUserBuilder};
pub use open_feature::FeatBitProvider;
pub use options::{FbOptions, FbOptionsBuilder};

/// The SDK name sent in network user-agent headers.
pub const SDK_NAME: &str = "featbit-rust-server-sdk";

/// The SDK package version.
pub const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

pub(crate) fn user_agent() -> String {
    format!("{SDK_NAME}/{SDK_VERSION}")
}
