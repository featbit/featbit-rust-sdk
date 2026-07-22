use std::sync::Arc;
use std::time::Duration;

use url::Url;

use crate::error::ConfigError;
use crate::model::DataSyncEnvelope;

use super::{FbOptions, FbOptionsBuilder, MAX_CONFIG_DURATION};

impl FbOptionsBuilder {
    /// Validates all fields and builds immutable options.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when a URL, duration, capacity, secret, retry policy, or bootstrap
    /// payload is invalid.
    pub fn build(self) -> Result<FbOptions, ConfigError> {
        self.validate_secret()?;
        let (streaming_url, event_url) = self.validated_urls()?;
        self.validate_runtime_limits()?;
        let bootstrap = self.validated_bootstrap()?;

        Ok(FbOptions {
            env_secret: Arc::from(self.env_secret),
            streaming_url,
            event_url,
            start_wait: self.start_wait,
            offline: self.offline,
            disable_events: self.disable_events,
            allow_track: self.allow_track,
            evaluation_observer: self.evaluation_observer,
            connect_timeout: self.connect_timeout,
            close_timeout: self.close_timeout,
            keep_alive_interval: self.keep_alive_interval,
            reconnect_delays: Arc::from(self.reconnect_delays),
            auto_flush_interval: self.auto_flush_interval,
            flush_timeout: self.flush_timeout,
            event_request_timeout: self.event_request_timeout,
            max_events_in_queue: self.max_events_in_queue,
            max_events_per_request: self.max_events_per_request,
            max_send_event_attempts: self.max_send_event_attempts,
            send_event_retry_interval: self.send_event_retry_interval,
            max_ws_message_size: self.max_ws_message_size,
            bootstrap,
        })
    }

    fn validate_secret(&self) -> Result<(), ConfigError> {
        let trimmed_secret = self.env_secret.trim_end_matches('=');
        if trimmed_secret.len() < 3
            || !trimmed_secret.is_ascii()
            || !trimmed_secret
                .bytes()
                .all(|byte| (0x21..=0x7e).contains(&byte))
        {
            return Err(ConfigError::InvalidEnvironmentSecret);
        }
        Ok(())
    }

    fn validated_urls(&self) -> Result<(Url, Url), ConfigError> {
        let streaming_url = parse_url("streaming_url", &self.streaming_url)?;
        if !matches!(streaming_url.scheme(), "ws" | "wss") {
            return Err(ConfigError::InvalidUrlScheme {
                field: "streaming_url",
                expected: "ws or wss",
            });
        }

        let event_url = parse_url("event_url", &self.event_url)?;
        if !matches!(event_url.scheme(), "http" | "https") {
            return Err(ConfigError::InvalidUrlScheme {
                field: "event_url",
                expected: "http or https",
            });
        }
        Ok((streaming_url, event_url))
    }

    fn validate_runtime_limits(&self) -> Result<(), ConfigError> {
        validate_nonzero_duration("start_wait", self.start_wait)?;
        validate_nonzero_duration("connect_timeout", self.connect_timeout)?;
        validate_nonzero_duration("close_timeout", self.close_timeout)?;
        validate_nonzero_duration("keep_alive_interval", self.keep_alive_interval)?;
        validate_nonzero_duration("auto_flush_interval", self.auto_flush_interval)?;
        validate_nonzero_duration("flush_timeout", self.flush_timeout)?;
        validate_nonzero_duration("event_request_timeout", self.event_request_timeout)?;
        if self.send_event_retry_interval > MAX_CONFIG_DURATION {
            return Err(ConfigError::InvalidDuration {
                field: "send_event_retry_interval",
                message: "must not exceed 365 days",
            });
        }
        if self.start_wait < self.connect_timeout {
            return Err(ConfigError::InvalidDuration {
                field: "start_wait",
                message: "must be greater than or equal to connect_timeout",
            });
        }
        if self.reconnect_delays.is_empty() {
            return Err(ConfigError::EmptyReconnectDelays);
        }
        if self.reconnect_delays.iter().all(Duration::is_zero) {
            return Err(ConfigError::InvalidDuration {
                field: "reconnect_delays",
                message: "must contain at least one non-zero delay",
            });
        }
        if self
            .reconnect_delays
            .iter()
            .any(|delay| *delay > MAX_CONFIG_DURATION)
        {
            return Err(ConfigError::InvalidDuration {
                field: "reconnect_delays",
                message: "individual delays must not exceed 365 days",
            });
        }

        validate_capacity("max_events_in_queue", self.max_events_in_queue, 1_000_000)?;
        validate_capacity(
            "max_events_per_request",
            self.max_events_per_request,
            10_000,
        )?;
        validate_capacity("max_send_event_attempts", self.max_send_event_attempts, 100)?;
        validate_capacity(
            "max_ws_message_size",
            self.max_ws_message_size,
            64 * 1024 * 1024,
        )?;
        Ok(())
    }

    fn validated_bootstrap(&self) -> Result<Option<Arc<DataSyncEnvelope>>, ConfigError> {
        if self.bootstrap_json.is_some() && !self.offline {
            return Err(ConfigError::BootstrapRequiresOffline);
        }
        let Some(json) = self.bootstrap_json.as_deref() else {
            return Ok(None);
        };
        let envelope = serde_json::from_str::<DataSyncEnvelope>(json)
            .map_err(|error| ConfigError::InvalidBootstrap(error.to_string()))?;
        if envelope.message_type != "data-sync" || envelope.data.event_type != "full" {
            return Err(ConfigError::InvalidBootstrap(
                "offline bootstrap must be a full data-sync envelope".to_owned(),
            ));
        }
        Ok(Some(Arc::new(envelope)))
    }
}

fn parse_url(field: &'static str, value: &str) -> Result<Url, ConfigError> {
    Url::parse(value)
        .map_err(|error| ConfigError::InvalidUrl {
            field,
            message: error.to_string(),
        })
        .and_then(|url| {
            let validation_message = if url.host_str().is_none() {
                Some("URL must contain a host")
            } else if !url.username().is_empty() || url.password().is_some() {
                Some("URL must not contain credentials")
            } else if url.query().is_some() {
                Some("URL must not contain a query")
            } else if url.fragment().is_some() {
                Some("URL must not contain a fragment")
            } else {
                None
            };
            if let Some(message) = validation_message {
                Err(ConfigError::InvalidUrl {
                    field,
                    message: message.to_owned(),
                })
            } else {
                Ok(url)
            }
        })
}

fn validate_nonzero_duration(field: &'static str, duration: Duration) -> Result<(), ConfigError> {
    if duration.is_zero() {
        Err(ConfigError::InvalidDuration {
            field,
            message: "must be greater than zero",
        })
    } else if duration > MAX_CONFIG_DURATION {
        Err(ConfigError::InvalidDuration {
            field,
            message: "must not exceed 365 days",
        })
    } else {
        Ok(())
    }
}

const fn validate_capacity(
    field: &'static str,
    value: usize,
    maximum: usize,
) -> Result<(), ConfigError> {
    if value == 0 {
        Err(ConfigError::InvalidCapacity {
            field,
            message: "must be greater than zero",
        })
    } else if value > maximum {
        Err(ConfigError::InvalidCapacity {
            field,
            message: "exceeds the supported safety limit",
        })
    } else {
        Ok(())
    }
}
