use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;

/// A builder for the DynamoDB table keys like partition key or sort key.
pub trait Key<'a> {
    /// Attribute name for partition key
    const PARTITION_KEY: &'a str;

    /// Attribute name for sort key
    const SORT_KEY: Option<&'a str>;

    /// Input types to get partition key.
    type PartitionInput;

    /// Input types to get sort key.
    type SortInput;

    /// Define how to create partition key value.
    fn partition_key(input: Self::PartitionInput) -> AttributeValue;

    /// Define how to create sort key value.
    ///
    /// If the table doesn't have the sort key, you must return None from this method.
    fn sort_key(input: Self::SortInput) -> Option<AttributeValue>;

    /// A map of attribute names to [`AttributeValue`] objects,
    /// representing the primary key of the table.
    fn key(pk: Self::PartitionInput, sk: Self::SortInput) -> HashMap<String, AttributeValue> {
        let mut map: HashMap<String, AttributeValue> = HashMap::new();

        map.insert(Self::PARTITION_KEY.into(), Self::partition_key(pk));

        if let (Some(sk_attr), Some(sk_val)) = (Self::SORT_KEY, Self::sort_key(sk)) {
            map.insert(sk_attr.into(), sk_val);
        }

        map
    }
}

/// Represents DynamoDB Table and you should implement this trait to the object to which you map
/// the DyanmoDB table.
pub trait DynamodbTable<'a> {
    /// The DynamoDB table name.
    const TABLE_NAME: &'a str;

    /// A KeyBuilder type.
    type Key: Key<'a>;

    /// Create inputs for partition key and sort key from the instance.
    fn key_inputs(
        &self,
    ) -> (
        <<Self as DynamodbTable<'a>>::Key as Key<'a>>::PartitionInput,
        <<Self as DynamodbTable<'a>>::Key as Key<'a>>::SortInput,
    );

    /// Create the primary key from the instance.
    fn key(&self) -> HashMap<String, AttributeValue> {
        let (pk, sk) = self.key_inputs();
        <<Self as DynamodbTable<'a>>::Key as Key>::key(pk, sk)
    }
}
