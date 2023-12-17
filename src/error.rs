use super::BoxError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("conversion failure from DynamoDB item into your object: {0}")]
    Conversion(#[source] BoxError),

    #[error(transparent)]
    Sdk(BoxError),
}
