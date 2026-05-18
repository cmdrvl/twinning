#![forbid(unsafe_code)]
#![cfg(feature = "rest")]

#[path = "rest/metadata_api_smoke.rs"]
mod metadata_api_smoke;

#[path = "rest/run_mode.rs"]
mod run_mode;

#[path = "rest/chaos.rs"]
mod chaos;
