mod core;
mod error;
pub mod operations;

pub use core::*;

/// Common error.
pub use error::Error;

/// Type alias of boxed error.
pub use aws_sdk_dynamodb::error::BoxError;
