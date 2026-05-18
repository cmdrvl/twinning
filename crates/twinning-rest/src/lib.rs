#![forbid(unsafe_code)]

pub use config::RestConfig;
pub use twinning_kernel::{backend, catalog, ir, kernel, refusal, result, runtime};

pub mod auth;
pub mod canary;
pub mod config;
pub mod encode;
pub mod listener;
#[cfg(feature = "mcp")]
pub mod mcp;
pub mod normalize;
pub mod policy;
pub mod report;
pub mod routes;
pub mod seed;
pub mod session;
pub mod session_log;
pub mod spec;
pub mod topology;
pub mod xext;

mod protocol {
    pub(crate) mod rest {
        #[allow(unused_imports)]
        pub(crate) use crate::{auth, policy, routes, session_log, spec, xext};
    }
}
