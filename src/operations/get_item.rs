use super::{BoxError, DynamodbTable, Error, Item, Key};

use aws_sdk_dynamodb::{
    operation::get_item::{builders::GetItemInputBuilder, GetItemInput},
    Client,
};
use std::collections::HashMap;
use std::marker::PhantomData;

/// A trait enables your objects to execute DynamoDB GetItem operation.
pub trait GetItem<'a>: DynamodbTable<'a> + TryFrom<Item, Error = BoxError> {
    fn get_item() -> GetItemOperation<'a, Self, Self::Key> {
        let input_builder = GetItemInput::builder()
            .table_name(Self::TABLE_NAME)
            .set_consistent_read(Self::consistent_read())
            .set_projection_expression(Self::projection_expression())
            .set_expression_attribute_names(Self::expression_attribute_names());

        GetItemOperation {
            key: None,
            input_builder,
            item: PhantomData,
            key_builder: PhantomData,
        }
    }

    /// Return value to be passed as `ConsistentRead` to [`GetItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ConsistentRead` option.
    fn consistent_read() -> Option<bool> {
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
pub struct GetItemOperation<'a, T, K>
where
    T: DynamodbTable<'a> + TryFrom<Item, Error = BoxError>,
    K: Key<'a>,
{
    key: Option<Item>,
    input_builder: GetItemInputBuilder,
    item: PhantomData<&'a T>,
    key_builder: PhantomData<&'a K>,
}

impl<'a, T, K> GetItemOperation<'a, T, K>
where
    T: DynamodbTable<'a> + TryFrom<Item, Error = BoxError>,
    K: Key<'a>,
{
    /// Set key.
    pub fn set_key(self, pk: K::PartitionInput, sk: K::SortInput) -> Self {
        Self {
            key: Some(K::key(pk, sk)),
            ..self
        }
    }

    /// Send GetItem request with given client object.
    pub async fn send(self, client: &Client) -> Result<Option<T>, Error> {
        self.input_builder
            .set_key(self.key)
            .send_with(client)
            .await
            .map_err(|err| Error::Sdk(Box::new(err)))?
            .item
            .map(T::try_from)
            .transpose()
            .map_err(Error::Conversion)
    }
}
