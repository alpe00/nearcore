use near_client_primitives::types::GetSplitStorageError;
use near_jsonrpc_primitives::{
    errors::RpcParseError,
    types::split_storage::{RpcSplitStorageError, RpcSplitStorageRequest},
};
use serde_json::Value;

use super::{parse_params, RpcFrom, RpcRequest};

impl RpcRequest for RpcSplitStorageRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<Self>(value)
    }
}

impl RpcFrom<actix::MailboxError> for RpcSplitStorageError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<GetSplitStorageError> for RpcSplitStorageError {
    fn rpc_from(error: GetSplitStorageError) -> Self {
        match error {
            GetSplitStorageError::IOError(error_message) => Self::InternalError { error_message },
            GetSplitStorageError::Unreachable(ref error_message) => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcSplitStorageError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}
