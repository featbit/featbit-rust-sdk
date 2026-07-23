//! `FeatBit`'s server-side SDK for Rust.
//!
//! The SDK keeps flag data synchronized in the background and evaluates flags locally. Create one
//! [`FbClient`] per `FeatBit` environment and reuse it for the lifetime of the application.

#![forbid(unsafe_code)]

mod client;
mod data_sync;
mod error;
mod evaluation;
mod events;
mod model;
mod observation;
mod options;
mod prepared;
mod store;
#[cfg(test)]
mod test_support;
mod worker;

pub use client::{
    ClientStatus, EvaluationDetail, EvaluationError, FbClient, RawEvaluation, ReasonKind,
};
pub use error::ConfigError;
pub use evaluation::EvaluationReason;
pub use events::FbEvaluationEvent;
pub use model::{FbUser, FbUserBuilder};
pub use observation::{
    EvaluationObservation, EvaluationObservationError, EvaluationObservationReason,
    EvaluationObserver,
};
pub use options::{FbOptions, FbOptionsBuilder};

/// The SDK name sent in network user-agent headers.
pub const SDK_NAME: &str = "featbit-rust-server-sdk";

/// The SDK package version.
pub const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

pub(crate) fn user_agent() -> String {
    format!("{SDK_NAME}/{SDK_VERSION}")
}
