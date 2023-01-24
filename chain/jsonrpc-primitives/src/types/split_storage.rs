use near_client_primitives::types::GetSplitStorageResult;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcSplitStorageRequest {}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcSplitStorageResponse {
    #[serde(flatten)]
    pub result: GetSplitStorageResult,
}

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcSplitStorageError {
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
}

impl From<RpcSplitStorageError> for crate::errors::RpcError {
    fn from(error: RpcSplitStorageError) -> Self {
        let error_data = match &error {
            RpcSplitStorageError::InternalError { .. } => Some(Value::String(error.to_string())),
        };

        let error_data_value = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcSplitStorageError: {:?}", err),
                )
            }
        };

        Self::new_internal_or_handler_error(error_data, error_data_value)
    }
}
