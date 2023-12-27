use super::{BoxError, DynamodbTable, Error, Item, Key};

use aws_sdk_dynamodb::{
    operation::delete_item::{builders::DeleteItemInputBuilder, DeleteItemInput},
    types::ReturnValue,
    Client,
};
use std::collections::HashMap;
use std::marker::PhantomData;

/// A trait enables your objects to execute DynamoDB DeleteItem operation.
pub trait DeleteItem<'a>: DynamodbTable<'a> + TryFrom<Item, Error = BoxError> {
    fn delete_item() -> DeleteItemOperation<'a, Self, Self::Key> {
        let input_builder = DeleteItemInput::builder()
            .table_name(Self::TABLE_NAME)
            .set_return_values(Self::return_values())
            .set_condition_expression(Self::condition_expression())
            .set_expression_attribute_names(Self::expression_attribute_names())
            .set_expression_attribute_values(Self::expression_attribute_values());

        DeleteItemOperation {
            key: None,
            input_builder,
            item: PhantomData,
            key_builder: PhantomData,
        }
    }

    /// Return value to be passed as `ReturnValues` to [`DeleteItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ReturnValues` option.
    /// If you want that the [`DeleteItemOperation::send`] returns some value, you
    /// have to overwrite this method to return `ALL_OLD`.
    /// Otherwise it returns None.
    fn return_values() -> Option<ReturnValue> {
        None
    }

    /// Return value to be passed as `ConditionExpression` to [`ConditionItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ConditionExpression` option.
    fn condition_expression() -> Option<String> {
        None
    }

    /// Return value to be passed as `ExpressionAttributeNames` to [`DeleteItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ExpressionAttributeNames` option.
    fn expression_attribute_names() -> Option<HashMap<String, String>> {
        None
    }

    /// Return value to be passed as `ExpressionAttributeValues` to [`DeleteItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ExpressionAttributeValues` option.
    fn expression_attribute_values() -> Option<Item> {
        None
    }
}

/// Represents the DynamoDB DeleteItem operation.
#[derive(Debug, Clone)]
pub struct DeleteItemOperation<'a, T, K>
where
    T: DynamodbTable<'a> + TryFrom<Item, Error = BoxError>,
    K: Key<'a>,
{
    key: Option<Item>,
    input_builder: DeleteItemInputBuilder,
    item: PhantomData<&'a T>,
    key_builder: PhantomData<&'a K>,
}

impl<'a, T, K> DeleteItemOperation<'a, T, K>
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

    /// Set condition expression
    pub fn set_condition_expression(self, expr: impl Into<String>) -> Self {
        Self {
            input_builder: self.input_builder.condition_expression(expr),
            ..self
        }
    }

    /// Set expression attribute names
    pub fn set_expression_attribute_names(self, names: HashMap<String, String>) -> Self {
        Self {
            input_builder: self
                .input_builder
                .set_expression_attribute_names(Some(names)),
            ..self
        }
    }

    /// Set expression attribute values
    pub fn set_expression_attribute_values(self, values: Item) -> Self {
        Self {
            input_builder: self
                .input_builder
                .set_expression_attribute_values(Some(values)),
            ..self
        }
    }

    pub async fn send(self, client: &Client) -> Result<Option<T>, Error> {
        let return_value = matches!(
            self.input_builder.get_return_values(),
            Some(ReturnValue::AllOld)
        );

        let output = self
            .input_builder
            .set_key(self.key)
            .send_with(client)
            .await
            .map_err(|err| Error::Sdk(Box::new(err)))?;

        if return_value {
            output
                .attributes
                .map(T::try_from)
                .transpose()
                .map_err(Error::Conversion)
        } else {
            Ok(None)
        }
    }
}
