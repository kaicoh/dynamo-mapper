#[macro_use]
mod macros;

mod error;
pub mod helpers;
pub mod operations;
mod table;

pub use table::*;

/// Common error.
pub use error::Error;

/// Type alias of boxed error.
pub use aws_sdk_dynamodb::error::BoxError;
