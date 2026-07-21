use thiserror::Error;

/// An invalid SDK configuration.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConfigError {
    /// The environment secret is empty or cannot be encoded into a `FeatBit` connection token.
    #[error("the environment secret must be a non-empty ASCII value of at least three characters")]
    InvalidEnvironmentSecret,

    /// A configured endpoint is not a valid URL.
    #[error("{field} is not a valid URL: {message}")]
    InvalidUrl {
        /// The name of the invalid option.
        field: &'static str,
        /// The parser or validation diagnostic. It never contains the environment secret.
        message: String,
    },

    /// A configured endpoint uses an unsupported URL scheme.
    #[error("{field} must use one of these schemes: {expected}")]
    InvalidUrlScheme {
        /// The name of the invalid option.
        field: &'static str,
        /// Supported schemes.
        expected: &'static str,
    },

    /// A duration has an invalid zero value or relationship to another duration.
    #[error("invalid duration for {field}: {message}")]
    InvalidDuration {
        /// The name of the invalid option.
        field: &'static str,
        /// A safe validation diagnostic.
        message: &'static str,
    },

    /// A queue, batch, or message capacity is outside its supported range.
    #[error("invalid capacity for {field}: {message}")]
    InvalidCapacity {
        /// The name of the invalid option.
        field: &'static str,
        /// A safe validation diagnostic.
        message: &'static str,
    },

    /// Reconnect cannot make progress without a delay policy.
    #[error("reconnect_delays must contain at least one duration")]
    EmptyReconnectDelays,

    /// Bootstrap data is only meaningful when no remote data source is running.
    #[error("bootstrap JSON can only be configured in offline mode")]
    BootstrapRequiresOffline,

    /// The bootstrap JSON did not contain a valid `FeatBit` data-sync envelope.
    #[error("invalid bootstrap JSON: {0}")]
    InvalidBootstrap(String),
}
