use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;

/// Type alias for DynamoDB item
pub type Item = HashMap<String, AttributeValue>;

/// A builder for the DynamoDB table keys like partition key or sort key.
pub trait KeyBuilder {
    /// A type or multiple types of which the key consists.
    ///
    /// If you need multiple types to build the key, use objects holding multiple types like tuple.
    type Inputs;

    /// Build the key value.
    ///
    /// If this returns `None`, it means the table doesn't have this kind of key.
    /// For example if your table doesn't have the sort key, you should set [`NoKey`] as
    /// SkBuilder of the [`DynamodbTable`] trait.
    ///
    /// The [`NoKey`] always returns `None` from the build method.
    fn build(inputs: Self::Inputs) -> Option<AttributeValue>;
}

/// A KeyBuilder object representing the absense of the kind of key.
pub struct NoKey;

impl KeyBuilder for NoKey {
    type Inputs = ();

    fn build(_: Self::Inputs) -> Option<AttributeValue> {
        None
    }
}

/// Represents DynamoDB Table and you should implement this trait to the object to which you map
/// the DyanmoDB table.
pub trait DynamodbTable {
    /// The DynamoDB table name.
    const TABLE_NAME: String;

    /// The attribute name for the partition key of the DynamoDB table.
    const PK_ATTRIBUTE: String;

    /// The attribute name for the sort key of the DynamoDB table.
    const SK_ATTRIBUTE: Option<String>;

    /// A KeyBuilder type for the partition key.
    type PkBuilder: KeyBuilder;

    /// A KeyBuilder type for the sort key.
    ///
    /// If your table doesn't have the sort key, use [`NoKey`] as this type.
    type SkBuilder: KeyBuilder;
}
