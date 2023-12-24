use super::{DynamodbTable, Error, Item};

use aws_sdk_dynamodb::{
    operation::put_item::{builders::PutItemInputBuilder, PutItemInput, PutItemOutput},
    types::{AttributeValue, ReturnValue},
    Client,
};
use std::collections::HashMap;

/// A trait enables your objects to execute DynamoDB PutItem operation.
pub trait PutItem<'a>: DynamodbTable<'a> + Into<Item> {
    fn put_item() -> PutItemOperation<Self> {
        let input_builder = PutItemInput::builder()
            .table_name(Self::TABLE_NAME)
            .set_return_values(Self::return_values())
            .set_condition_expression(Self::condition_expression())
            .set_expression_attribute_names(Self::expression_attribute_names())
            .set_expression_attribute_values(Self::expression_attribute_values());

        PutItemOperation {
            item: None,
            input_builder,
        }
    }

    /// Return values to be passed as `ReturnValues` to [`PutItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ReturnValues` option.
    /// Only `None` or `ALL_OLD` are valid. If you set `ALL_OLD` as this value,
    /// the [`PutItemOperation::send`] returns an old object as Option<T> otherwise
    /// it returns None as Option<T>.
    fn return_values() -> Option<ReturnValue> {
        None
    }

    /// Return values to be passed as `ConditionExpression` to [`PutItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ConditionExpression` option.
    fn condition_expression() -> Option<String> {
        None
    }

    /// Return values to be passed as `ExpressionAttributeNames` to [`PutItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ExpressionAttributeNames` option.
    fn expression_attribute_names() -> Option<HashMap<String, String>> {
        None
    }

    /// Return values to be passed as `ExpressionAttributeValues` to [`PutItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ExpressionAttributeValues` option.
    fn expression_attribute_values() -> Option<HashMap<String, AttributeValue>> {
        None
    }
}

/// Represents the DynamoDB PutItem operation.
#[derive(Debug, Clone)]
pub struct PutItemOperation<T>
where
    T: Into<Item>,
{
    item: Option<T>,
    input_builder: PutItemInputBuilder,
}

impl<T> PutItemOperation<T>
where
    T: Into<Item>,
{
    pub fn set_item(self, item: T) -> Self {
        Self {
            item: Some(item),
            ..self
        }
    }

    pub async fn send(self, client: &Client) -> Result<PutItemOutput, Error> {
        self.input_builder
            .set_item(self.item.map(|v| v.into()))
            .send_with(client)
            .await
            .map_err(|err| Error::Sdk(Box::new(err)))
    }
}
