#![forbid(unsafe_code)]
#![cfg(feature = "rest")]

#[path = "rest/metadata_api_smoke.rs"]
mod metadata_api_smoke;

#[path = "rest/run_mode.rs"]
mod run_mode;

#[path = "rest/chaos.rs"]
mod chaos;

#[path = "rest/response_stubs.rs"]
mod response_stubs;

#[path = "rest/request_validation.rs"]
mod request_validation;

#[path = "rest/openfigi_v2_v3.rs"]
mod openfigi_v2_v3;
