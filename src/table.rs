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
    /// For example if your table doesn't have the sort key, you should set [`NotKey`] as
    /// SkBuilder of the [`DynamodbTable`] trait.
    ///
    /// The [`NotKey`] always returns `None` from the build method.
    fn build(inputs: Self::Inputs) -> Option<AttributeValue>;
}

/// A KeyBuilder object representing the absense of the kind of key.
pub struct NotKey;

impl KeyBuilder for NotKey {
    type Inputs = ();

    fn build(_: Self::Inputs) -> Option<AttributeValue> {
        None
    }
}

/// Represents DynamoDB Table and you should implement this trait to the object to which you map
/// the DyanmoDB table.
pub trait DynamodbTable<'a> {
    /// The DynamoDB table name.
    const TABLE_NAME: &'a str;

    /// The attribute name for the partition key of the DynamoDB table.
    const PK_ATTRIBUTE: &'a str;

    /// The attribute name for the sort key of the DynamoDB table.
    const SK_ATTRIBUTE: Option<&'a str>;

    /// A KeyBuilder type for the partition key.
    type PkBuilder: KeyBuilder;

    /// A KeyBuilder type for the sort key.
    ///
    /// If your table doesn't have the sort key, use [`NotKey`] as this type.
    type SkBuilder: KeyBuilder;
}

#[cfg(test)]
pub mod test_tables {
    use super::super::{helpers::attribute_value::AttributeMap, BoxError};
    use super::*;

    #[derive(Debug, Clone)]
    pub struct SingleKey {
        id: u32,
        name: String,
    }

    impl SingleKey {
        pub fn new(id: u32, name: impl Into<String>) -> Self {
            Self {
                id,
                name: name.into(),
            }
        }
    }

    impl<'a> DynamodbTable<'a> for SingleKey {
        const TABLE_NAME: &'a str = "SingleKey";
        const PK_ATTRIBUTE: &'a str = "pk";
        const SK_ATTRIBUTE: Option<&'a str> = None;
        type PkBuilder = PkBuilder;
        type SkBuilder = NotKey;
    }

    impl TryFrom<Item> for SingleKey {
        type Error = BoxError;

        fn try_from(item: Item) -> Result<Self, Self::Error> {
            let map = AttributeMap::from(item);
            Ok(SingleKey {
                id: map.n("id").unwrap().parse().unwrap(),
                name: map.s("name").unwrap().into(),
            })
        }
    }

    impl From<SingleKey> for Item {
        fn from(value: SingleKey) -> Item {
            let SingleKey { id, name } = value;
            AttributeMap::new()
                .set("pk", PkBuilder::build(id).unwrap())
                .set_n("id", id.to_string())
                .set_s("name", name)
                .into_item()
        }
    }

    #[derive(Debug, Clone)]
    pub struct CompositeKey {
        id: u32,
        name: String,
    }

    impl CompositeKey {
        pub fn new(id: u32, name: impl Into<String>) -> Self {
            Self {
                id,
                name: name.into(),
            }
        }
    }

    impl<'a> DynamodbTable<'a> for CompositeKey {
        const TABLE_NAME: &'a str = "CompositeKey";
        const PK_ATTRIBUTE: &'a str = "pk";
        const SK_ATTRIBUTE: Option<&'a str> = Some("sk");
        type PkBuilder = PkBuilder;
        type SkBuilder = SkBuilder;
    }

    impl TryFrom<Item> for CompositeKey {
        type Error = BoxError;

        fn try_from(item: Item) -> Result<Self, Self::Error> {
            let map = AttributeMap::from(item);
            Ok(CompositeKey {
                id: map.n("id").unwrap().parse().unwrap(),
                name: map.s("name").unwrap().into(),
            })
        }
    }

    impl From<CompositeKey> for Item {
        fn from(value: CompositeKey) -> Item {
            let CompositeKey { id, name } = value;
            AttributeMap::new()
                .set("pk", PkBuilder::build(id).unwrap())
                .set("sk", SkBuilder::build(name.clone()).unwrap())
                .set_n("id", id.to_string())
                .set_s("name", name)
                .into_item()
        }
    }

    pub struct PkBuilder;
    impl KeyBuilder for PkBuilder {
        type Inputs = u32;

        fn build(inputs: Self::Inputs) -> Option<AttributeValue> {
            Some(AttributeValue::S(format!("{inputs}")))
        }
    }

    pub struct SkBuilder;
    impl KeyBuilder for SkBuilder {
        type Inputs = String;

        fn build(inputs: Self::Inputs) -> Option<AttributeValue> {
            Some(AttributeValue::S(format!("{inputs}")))
        }
    }
}
