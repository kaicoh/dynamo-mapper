use super::{BoxError, DynamodbTable, Error, Item, KeyBuilder};

use aws_sdk_dynamodb::{
    operation::get_item::{builders::GetItemInputBuilder, GetItemInput},
    types::{AttributeValue, ReturnConsumedCapacity},
    Client,
};
use std::collections::HashMap;
use std::marker::PhantomData;

/// A trait enables your objects to execute DynamoDB GetItem operation.
pub trait GetItem<'a>: DynamodbTable<'a> + TryFrom<Item, Error = BoxError> {
    fn get_item() -> GetItemOperation<Self, Self::PkBuilder, Self::SkBuilder> {
        let input_builder = GetItemInput::builder()
            .table_name(Self::TABLE_NAME)
            .set_attributes_to_get(Self::attribute_to_get())
            .set_consistent_read(Self::consistent_read())
            .set_return_consumed_capacity(Self::return_consumed_capacity())
            .set_projection_expression(Self::projection_expression())
            .set_expression_attribute_names(Self::expression_attribute_names());

        GetItemOperation {
            pk_attribute: Self::PK_ATTRIBUTE.to_string(),
            sk_attribute: Self::SK_ATTRIBUTE.map(|v| v.to_string()),
            pk: None,
            sk: None,
            input_builder,
            item: PhantomData,
            pk_builder: PhantomData,
            sk_builder: PhantomData,
        }
    }

    /// Return values to be passed as `AttributeToGet` to [`GetItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `AttributeToGet` option.
    fn attribute_to_get() -> Option<Vec<String>> {
        None
    }

    /// Return value to be passed as `ConsistentRead` to [`GetItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ConsistentRead` option.
    fn consistent_read() -> Option<bool> {
        None
    }

    /// Return value to be passed as `ReturnConsumedCapacity` to [`GetItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ReturnConsumedCapacity` option.
    fn return_consumed_capacity() -> Option<ReturnConsumedCapacity> {
        None
    }

    /// Return value to be passed as `ProjectionExpression` to [`GetItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ProjectionExpression` option.
    fn projection_expression() -> Option<String> {
        None
    }

    /// Return value to be passed as `ExpressionAttributeNames` to [`GetItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ExpressionAttributeNames` option.
    fn expression_attribute_names() -> Option<HashMap<String, String>> {
        None
    }
}

/// Represents the DynamoDB GetItem operation.
#[derive(Debug, Clone)]
pub struct GetItemOperation<T, PkBuilder, SkBuilder>
where
    T: TryFrom<Item, Error = BoxError>,
    PkBuilder: KeyBuilder,
    SkBuilder: KeyBuilder,
{
    pk_attribute: String,
    sk_attribute: Option<String>,
    pk: Option<AttributeValue>,
    sk: Option<AttributeValue>,
    input_builder: GetItemInputBuilder,
    item: PhantomData<T>,
    pk_builder: PhantomData<PkBuilder>,
    sk_builder: PhantomData<SkBuilder>,
}

impl<T, PkBuilder, SkBuilder> GetItemOperation<T, PkBuilder, SkBuilder>
where
    T: TryFrom<Item, Error = BoxError>,
    PkBuilder: KeyBuilder,
    SkBuilder: KeyBuilder,
{
    /// Set partition key.
    pub fn set_pk(self, inputs: PkBuilder::Inputs) -> Self {
        Self {
            pk: PkBuilder::build(inputs),
            ..self
        }
    }

    /// Set sort key.
    pub fn set_sk(self, inputs: SkBuilder::Inputs) -> Self {
        Self {
            sk: SkBuilder::build(inputs),
            ..self
        }
    }

    /// Send GetItem request with given client object.
    pub async fn send(self, client: &Client) -> Result<Option<T>, Error> {
        let keys = self.keys();

        self.input_builder
            .set_key(Some(keys))
            .send_with(client)
            .await
            .map_err(|err| Error::Sdk(Box::new(err)))?
            .item
            .map(T::try_from)
            .transpose()
            .map_err(Error::Conversion)
    }

    /// Return GetItem keys.
    /// Panic when the partition key is not set.
    fn keys(&self) -> Item {
        let mut item: Item = HashMap::new();

        if let Some(pk) = self.pk.as_ref() {
            item.insert(self.pk_attribute.clone(), pk.clone());
        } else {
            panic!("Partition key is not set");
        }

        if let (Some(sk_attr), Some(sk)) = (self.sk_attribute.as_ref(), self.sk.as_ref()) {
            item.insert(sk_attr.clone(), sk.clone());
        }

        item
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_tables::*;
    use super::*;

    mod single_key {
        use super::*;

        impl<'a> GetItem<'a> for SingleKey {}

        #[test]
        fn it_creates_key_from_partition_key() {
            let ope = SingleKey::get_item();
            let keys = ope.set_pk(100).keys();
            assert_eq!(keys.get("pk"), Some(&AttributeValue::S("100".into())));
            assert!(keys.get("sk").is_none());
        }
    }

    mod composite_key {
        use super::*;

        impl<'a> GetItem<'a> for CompositeKey {}

        #[test]
        fn it_creates_key_from_partition_key_and_sort_key() {
            let ope = CompositeKey::get_item();
            let keys = ope.set_pk(100).keys();
            assert_eq!(keys.get("pk"), Some(&AttributeValue::S("100".into())));
            assert!(keys.get("sk").is_none());

            let ope = CompositeKey::get_item();
            let keys = ope.set_pk(100).set_sk("foo".into()).keys();
            assert_eq!(keys.get("pk"), Some(&AttributeValue::S("100".into())));
            assert_eq!(keys.get("sk"), Some(&AttributeValue::S("foo".into())));
        }
    }
}
