use axum_derive_error::ErrorResponse;
use serde::{Deserialize, Serialize};
use thiserror::Error as ThisError;
#[derive(Serialize, Deserialize, Clone, ThisError, ErrorResponse)]
pub enum Error {
    #[status(axum::http::StatusCode::BAD_REQUEST)]
    CustomBadRequest(&'static str),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::CustomBadRequest(s) => write!(f, "Bad request: {}", s),
        }
    }
}
