use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use featbit_server_sdk::{ClientStatus, FeatBitProvider};
use open_feature::{Client as OpenFeatureClient, EvaluationContext};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time;

use super::api::TestFlag;
use super::{failure, TestResult};

#[derive(Clone)]
pub(super) struct AppState {
    pub(super) flags: Arc<OpenFeatureClient>,
    pub(super) provider: FeatBitProvider,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct EvaluationRequest {
    targeting_key: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    attributes: BTreeMap<String, String>,
    #[serde(default)]
    default_value: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct EvaluationResponse {
    pub(super) value: bool,
    pub(super) variation_id: Option<String>,
    pub(super) reason: String,
    pub(super) used_fallback: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthResponse {
    status: &'static str,
    initialized: bool,
}

async fn evaluate_boolean(
    State(state): State<AppState>,
    Path(flag_key): Path<String>,
    Json(request): Json<EvaluationRequest>,
) -> Json<EvaluationResponse> {
    let EvaluationRequest {
        targeting_key,
        name,
        attributes,
        default_value,
    } = request;
    let mut context = EvaluationContext::default().with_targeting_key(targeting_key);
    context.add_custom_field("name", name);
    for (field, value) in attributes {
        if field != "name" {
            context.add_custom_field(field, value);
        }
    }

    let response = match state
        .flags
        .get_bool_details(&flag_key, Some(&context), None)
        .await
    {
        Ok(details) => EvaluationResponse {
            value: details.value,
            variation_id: details.variant,
            reason: details
                .reason
                .map_or_else(|| "UNKNOWN".to_owned(), |reason| reason.to_string()),
            used_fallback: false,
        },
        Err(error) => EvaluationResponse {
            value: default_value,
            variation_id: None,
            reason: error.code.to_string(),
            used_fallback: true,
        },
    };
    Json(response)
}

async fn readiness(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    let status = state.provider.client().status();
    let (http_status, text) = match status {
        ClientStatus::Ready => (StatusCode::OK, "ready"),
        ClientStatus::Stale => (StatusCode::OK, "stale"),
        ClientStatus::NotReady => (StatusCode::SERVICE_UNAVAILABLE, "not-ready"),
        ClientStatus::Closed => (StatusCode::SERVICE_UNAVAILABLE, "closed"),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "unknown"),
    };
    (
        http_status,
        Json(HealthResponse {
            status: text,
            initialized: state.provider.client().initialized(),
        }),
    )
}

pub(super) struct RunningApplication {
    address: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), io::Error>>,
}

impl RunningApplication {
    pub(super) async fn start(state: AppState) -> TestResult<Self> {
        let router = Router::new()
            .route("/health/ready", get(readiness))
            .route("/api/flags/{flag_key}/evaluate", post(evaluate_boolean))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown, receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ignored = receiver.await;
                })
                .await
        });
        Ok(Self {
            address,
            shutdown: Some(shutdown),
            task,
        })
    }

    pub(super) async fn stop(mut self) -> TestResult<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ignored = shutdown.send(());
        }
        match time::timeout(Duration::from_secs(5), self.task).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => Err(error.into()),
            Ok(Err(error)) => Err(error.into()),
            Err(_) => Err(failure("Axum did not stop within five seconds")),
        }
    }

    pub(super) fn evaluation_url(&self, flag: &TestFlag) -> String {
        format!("http://{}/api/flags/{}/evaluate", self.address, flag.key)
    }
}

#[derive(Clone)]
pub(super) struct Probe {
    targeting_key: String,
    name: String,
    attributes: BTreeMap<String, String>,
}

impl Probe {
    pub(super) fn user(targeting_key: impl Into<String>) -> Self {
        Self {
            targeting_key: targeting_key.into(),
            name: "Cloud test user".to_owned(),
            attributes: BTreeMap::new(),
        }
    }

    pub(super) fn with_attribute(mut self, name: &str, value: &str) -> Self {
        self.attributes.insert(name.to_owned(), value.to_owned());
        self
    }

    pub(super) fn request(&self) -> EvaluationRequest {
        EvaluationRequest {
            targeting_key: self.targeting_key.clone(),
            name: self.name.clone(),
            attributes: self.attributes.clone(),
            default_value: false,
        }
    }
}
