//! HTTP metrics middleware for Prometheus.
//!
//! Provides per-route latency histograms (for P95/P99 calculations) and request counters (for RPS).

use axum::{
    body::Body,
    extract::MatchedPath,
    http::Request,
    middleware::Next,
    response::IntoResponse,
};
use metrics::{counter, histogram};
use std::time::Instant;

/// Metric name for total HTTP requests (counter).
pub const HTTP_REQUESTS_TOTAL: &str = "http_requests_total";

/// Metric name for HTTP request duration in seconds (histogram).
pub const HTTP_REQUEST_DURATION_SECONDS: &str = "http_request_duration_seconds";

/// Axum middleware that records HTTP request metrics.
///
/// For each request, this records:
/// - `http_requests_total{method, path, status}` - incremented by 1
/// - `http_request_duration_seconds{method, path, status}` - request latency in seconds
///
/// Use with `axum::middleware::from_fn`:
/// ```ignore
/// Router::new()
///     .route("/foo", get(handler))
///     .layer(axum::middleware::from_fn(track_http_metrics))
/// ```
pub async fn track_http_metrics(request: Request<Body>, next: Next) -> impl IntoResponse {
    let start = Instant::now();

    let method = request.method().as_str().to_owned();

    // Extract the matched route pattern (e.g., "/api/v2/swap/:id") rather than the actual path.
    // This prevents high-cardinality labels from dynamic path segments like UUIDs.
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str().to_owned())
        .unwrap_or_else(|| request.uri().path().to_owned());

    let response = next.run(request).await;

    let status = response.status().as_u16().to_string();
    let latency = start.elapsed().as_secs_f64();

    counter!(
        HTTP_REQUESTS_TOTAL,
        "method" => method.clone(),
        "path" => path.clone(),
        "status" => status.clone(),
    )
    .increment(1);

    histogram!(
        HTTP_REQUEST_DURATION_SECONDS,
        "method" => method,
        "path" => path,
        "status" => status,
    )
    .record(latency);

    response
}


