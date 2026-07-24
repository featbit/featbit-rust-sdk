//! Evaluates flags directly through `FbClient` in Axum and closes `FeatBit` gracefully.
//!
//! The evaluation endpoint accepts both POST JSON and browser-friendly GET query parameters.

use std::collections::BTreeMap;
use std::env;
use std::error::Error;

use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use featbit_server_sdk::{ClientStatus, FbClient, FbOptions, FbUser};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

type AnyError = Box<dyn Error + Send + Sync>;

#[derive(Clone)]
struct AppState {
    // FbClient is a cheap, thread-safe handle. Every clone shares the same synchronized snapshot
    // and background workers.
    flags: FbClient,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BooleanEvaluationRequest {
    targeting_key: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    attributes: BTreeMap<String, String>,
    #[serde(default)]
    default_value: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BooleanEvaluationQuery {
    #[serde(default = "default_browser_targeting_key")]
    targeting_key: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    default_value: bool,
    // Any non-reserved query parameter becomes a FeatBit custom user attribute. For example,
    // `?country=CN&plan=pro` evaluates with both attributes.
    #[serde(flatten)]
    attributes: BTreeMap<String, String>,
}

impl From<BooleanEvaluationQuery> for BooleanEvaluationRequest {
    fn from(query: BooleanEvaluationQuery) -> Self {
        Self {
            targeting_key: query.targeting_key,
            name: query.name,
            attributes: query.attributes,
            default_value: query.default_value,
        }
    }
}

fn default_browser_targeting_key() -> String {
    "browser-example-user".to_owned()
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BooleanEvaluationResponse {
    flag_key: String,
    value: bool,
    variation_id: Option<String>,
    reason: String,
    used_fallback: bool,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    initialized: bool,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: &'static str,
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,tower_http=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()?;

    let options = FbOptions::builder(env::var("FEATBIT_ENV_SECRET")?)
        .streaming_url(env::var("FEATBIT_STREAMING_URL")?)
        .event_url(env::var("FEATBIT_EVENT_URL")?)
        .build()?;

    // Client construction can wait for initial flag data, so keep that bounded blocking wait off
    // Tokio's asynchronous worker threads.
    let flags = tokio::task::spawn_blocking(move || FbClient::with_options(options)).await?;
    let app = routes().with_state(AppState {
        flags: flags.clone(),
    });

    let bind_address =
        env::var("AXUM_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());
    let listener = TcpListener::bind(&bind_address).await?;
    let local_address = listener.local_addr()?;
    tracing::info!(address = %local_address, "FbClient Axum example listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Axum has drained in-flight requests. Close FeatBit without blocking an asynchronous worker
    // thread; close performs a bounded final event flush.
    tokio::task::spawn_blocking(move || flags.close()).await?;
    tracing::info!("FbClient Axum example stopped");
    Ok(())
}

fn routes() -> Router<AppState> {
    Router::new()
        .route("/health/ready", get(readiness))
        .route(
            "/api/flags/{flag_key}/evaluate",
            get(evaluate_boolean_get).post(evaluate_boolean_post),
        )
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(TraceLayer::new_for_http())
}

async fn readiness(State(state): State<AppState>) -> impl IntoResponse {
    let client_status = state.flags.status();
    let (http_status, status) = match client_status {
        ClientStatus::Ready => (StatusCode::OK, "ready"),
        // Cached data remains safe to evaluate while the synchronizer reconnects.
        ClientStatus::Stale => (StatusCode::OK, "stale"),
        ClientStatus::NotReady => (StatusCode::SERVICE_UNAVAILABLE, "not-ready"),
        ClientStatus::Closed => (StatusCode::SERVICE_UNAVAILABLE, "closed"),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "unknown"),
    };
    (
        http_status,
        Json(HealthResponse {
            status,
            initialized: state.flags.initialized(),
        }),
    )
}

async fn evaluate_boolean_get(
    State(state): State<AppState>,
    Path(flag_key): Path<String>,
    Query(query): Query<BooleanEvaluationQuery>,
) -> Result<Json<BooleanEvaluationResponse>, ApiError> {
    evaluate_boolean_request(&state, flag_key, query.into())
}

async fn evaluate_boolean_post(
    State(state): State<AppState>,
    Path(flag_key): Path<String>,
    // Body extractors belong last because they consume the request body.
    Json(request): Json<BooleanEvaluationRequest>,
) -> Result<Json<BooleanEvaluationResponse>, ApiError> {
    evaluate_boolean_request(&state, flag_key, request)
}

fn evaluate_boolean_request(
    state: &AppState,
    flag_key: String,
    request: BooleanEvaluationRequest,
) -> Result<Json<BooleanEvaluationResponse>, ApiError> {
    if request.targeting_key.trim().is_empty() {
        return Err(ApiError::invalid_targeting_key());
    }

    let BooleanEvaluationRequest {
        targeting_key,
        name,
        attributes,
        default_value,
    } = request;

    // In a real authenticated service, derive the targeting key from the authenticated identity
    // instead of trusting a caller-controlled identifier.
    let mut user = FbUser::builder(targeting_key).name(name);
    for (attribute, value) in attributes {
        if attribute != "name" {
            user = user.custom(attribute, value);
        }
    }
    let user = user.build();

    let detail = state
        .flags
        .bool_variation_detail(&flag_key, &user, default_value);
    let used_fallback = detail.variation_id.is_empty();
    let variation_id = (!used_fallback).then_some(detail.variation_id);

    Ok(Json(BooleanEvaluationResponse {
        flag_key,
        value: detail.value,
        variation_id,
        reason: detail.reason,
        used_fallback,
    }))
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: &'static str,
}

impl ApiError {
    const fn invalid_targeting_key() -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            message: "targetingKey must not be empty",
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(%error, "failed to install Ctrl+C signal handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::warn!(%error, "failed to install SIGTERM signal handler");
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
    tracing::info!("shutdown signal received; draining HTTP requests");
}

#[cfg(test)]
mod tests {
    use axum::http::Uri;

    use super::*;

    #[test]
    fn get_query_uses_browser_safe_defaults() {
        let uri = Uri::from_static("/api/flags/example-flag/evaluate");
        let Query(query) =
            Query::<BooleanEvaluationQuery>::try_from_uri(&uri).expect("query should deserialize");

        assert_eq!(query.targeting_key, "browser-example-user");
        assert!(query.name.is_empty());
        assert!(!query.default_value);
        assert!(query.attributes.is_empty());
    }

    #[test]
    fn get_query_maps_extra_parameters_to_custom_attributes() {
        let uri = Uri::from_static(
            "/api/flags/example-flag/evaluate?targetingKey=user-42&name=Browser%20User&defaultValue=true&country=CN&plan=pro",
        );
        let Query(query) =
            Query::<BooleanEvaluationQuery>::try_from_uri(&uri).expect("query should deserialize");

        assert_eq!(query.targeting_key, "user-42");
        assert_eq!(query.name, "Browser User");
        assert!(query.default_value);
        assert_eq!(query.attributes.get("country"), Some(&"CN".to_owned()));
        assert_eq!(query.attributes.get("plan"), Some(&"pro".to_owned()));
    }
}
