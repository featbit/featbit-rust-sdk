//! Evaluates flags through `OpenFeature` in Axum and closes `FeatBit` gracefully.

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use featbit_server_sdk::{ClientStatus, FbOptions, FeatBitProvider};
use open_feature::{Client as OpenFeatureClient, EvaluationContext, OpenFeature};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

type AnyError = Box<dyn Error + Send + Sync>;

#[derive(Clone)]
struct AppState {
    // OpenFeature's client is the application-facing evaluation API. It is shared because the
    // OpenFeature 0.3 client is not itself Clone.
    flags: Arc<OpenFeatureClient>,
    // Keep the provider only for FeatBit-specific readiness and explicit lifecycle operations.
    // Flag evaluation below never bypasses OpenFeature.
    provider: FeatBitProvider,
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

    // Provider construction can wait for initial flag data, so keep that bounded blocking wait
    // off Tokio's asynchronous worker threads.
    let provider = tokio::task::spawn_blocking(move || FeatBitProvider::new(options)).await?;
    let flags = {
        let mut api = OpenFeature::singleton_mut().await;
        api.set_provider(provider.clone()).await;
        Arc::new(api.create_client())
    };
    let app = routes().with_state(AppState {
        flags,
        provider: provider.clone(),
    });

    let bind_address =
        env::var("AXUM_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());
    let listener = TcpListener::bind(&bind_address).await?;
    let local_address = listener.local_addr()?;
    tracing::info!(address = %local_address, "Axum example listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Axum has drained in-flight requests. Unregister the OpenFeature provider, then flush events
    // and close FeatBit without blocking an asynchronous worker thread.
    OpenFeature::singleton_mut().await.shutdown().await;
    tokio::task::spawn_blocking(move || provider.client().close()).await?;
    tracing::info!("Axum example stopped");
    Ok(())
}

// Return a router that still needs AppState. Supplying state at the application boundary keeps
// route composition and testing straightforward.
fn routes() -> Router<AppState> {
    Router::new()
        .route("/health/ready", get(readiness))
        .route("/api/flags/{flag_key}/evaluate", post(evaluate_boolean))
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(TraceLayer::new_for_http())
}

async fn readiness(State(state): State<AppState>) -> impl IntoResponse {
    let featbit = state.provider.client();
    let client_status = featbit.status();
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
            initialized: featbit.initialized(),
        }),
    )
}

async fn evaluate_boolean(
    State(state): State<AppState>,
    Path(flag_key): Path<String>,
    // Body extractors belong last because they consume the request body.
    Json(request): Json<BooleanEvaluationRequest>,
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
    let mut context = EvaluationContext::default().with_targeting_key(targeting_key);
    for (attribute, value) in attributes {
        if attribute != "name" {
            context.add_custom_field(attribute, value);
        }
    }
    context.add_custom_field("name", name);

    let response = match state
        .flags
        .get_bool_details(&flag_key, Some(&context), None)
        .await
    {
        Ok(details) => BooleanEvaluationResponse {
            flag_key,
            value: details.value,
            variation_id: details.variant,
            reason: details
                .reason
                .map_or_else(|| "UNKNOWN".to_owned(), |reason| reason.to_string()),
            used_fallback: false,
        },
        Err(error) => {
            tracing::debug!(flag_key, code = %error.code, "flag evaluation used its fallback");
            BooleanEvaluationResponse {
                flag_key,
                value: default_value,
                variation_id: None,
                reason: error.code.to_string(),
                used_fallback: true,
            }
        }
    };

    Ok(Json(response))
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
