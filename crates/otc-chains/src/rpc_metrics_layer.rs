use std::task::{Context, Poll};
use std::time::Instant;
use tower::{Layer, Service};
use alloy::rpc::json_rpc::{RequestPacket, ResponsePacket};
use metrics::{histogram, counter};
use std::future::Future;
use std::pin::Pin;

#[derive(Clone)]
pub struct RpcMetricsLayer {
    pub chain: String,
}

impl RpcMetricsLayer {
    pub fn new(chain: String) -> Self {
        Self { chain }
    }
}

impl<S> Layer<S> for RpcMetricsLayer {
    type Service = RpcMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RpcMetricsService { 
            inner,
            chain: self.chain.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RpcMetricsService<S> {
    inner: S,
    chain: String,
}

impl<S> Service<RequestPacket> for RpcMetricsService<S>
where
    S: Service<RequestPacket, Response = ResponsePacket> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: std::fmt::Display + Send + 'static, // Relaxed bound for Error if possible, but alloy errors are usually Display
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        // Extract method name
        let method = match &req {
            RequestPacket::Single(call) => call.method().to_string(),
            RequestPacket::Batch(calls) => {
                if calls.is_empty() {
                    "empty_batch".to_string()
                } else {
                     "batch".to_string()
                }
            }
        };

        let start = Instant::now();
        let chain = self.chain.clone();
        

        let fut = self.inner.call(req);

        Box::pin(async move {
            let result = fut.await;
            let duration = start.elapsed();
            
            let status = if result.is_ok() { "success" } else { "error" };
            
            counter!("ethereum_rpc_requests_total", "method" => method.clone(), "status" => status, "chain" => chain.clone()).increment(1);
            histogram!("ethereum_rpc_duration_seconds", "method" => method, "status" => status, "chain" => chain).record(duration.as_secs_f64());
            
            result
        })
    }
}

