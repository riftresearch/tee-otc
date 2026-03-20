use std::time::Duration;

use metrics::histogram;
use otc_server::api::{
    DepositObservationAcceptedResponse, DepositObservationErrorResponse, DepositObservationRequest,
};
use reqwest::StatusCode;
use snafu::ResultExt;
use uuid::Uuid;

use crate::{
    config::SauronArgs,
    error::{OtcRejectedSnafu, OtcRequestSnafu, OtcUrlSnafu, Result},
};

const SAURON_OTC_SUBMISSION_DURATION_SECONDS: &str = "sauron_otc_submission_duration_seconds";

#[derive(Clone)]
pub struct OtcClient {
    client: reqwest::Client,
    base_url: String,
    detector_api_id: String,
    detector_api_secret: String,
}

impl OtcClient {
    pub fn new(args: &SauronArgs) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context(OtcRequestSnafu)?;

        Ok(Self {
            client,
            base_url: args.otc_internal_base_url.clone(),
            detector_api_id: args.otc_detector_api_id.clone(),
            detector_api_secret: args.otc_detector_api_secret.clone(),
        })
    }

    pub async fn submit_deposit_observation(
        &self,
        swap_id: Uuid,
        request: &DepositObservationRequest,
    ) -> Result<DepositObservationAcceptedResponse> {
        let started = std::time::Instant::now();
        let endpoint = reqwest::Url::parse(&self.base_url)
            .context(OtcUrlSnafu {
                base_url: self.base_url.clone(),
            })?
            .join(&format!("/api/v1/swaps/{swap_id}/deposit-observation"))
            .context(OtcUrlSnafu {
                base_url: self.base_url.clone(),
            })?;

        let response = self
            .client
            .post(endpoint)
            .header("X-API-ID", &self.detector_api_id)
            .header("X-API-SECRET", &self.detector_api_secret)
            .json(request)
            .send()
            .await
            .context(OtcRequestSnafu)?;

        match response.status() {
            StatusCode::OK | StatusCode::ACCEPTED => {
                histogram!(SAURON_OTC_SUBMISSION_DURATION_SECONDS, "status" => response.status().as_str().to_string())
                    .record(started.elapsed().as_secs_f64());
                response.json().await.context(OtcRequestSnafu)
            }
            _ => {
                let status = response.status();
                histogram!(SAURON_OTC_SUBMISSION_DURATION_SECONDS, "status" => status.as_str().to_string())
                    .record(started.elapsed().as_secs_f64());
                let body = response.text().await.unwrap_or_default();
                let body = match serde_json::from_str::<DepositObservationErrorResponse>(&body) {
                    Ok(parsed) => serde_json::to_string(&parsed).unwrap_or(body),
                    Err(_) => body,
                };
                OtcRejectedSnafu { status, body }.fail()
            }
        }
    }
}
