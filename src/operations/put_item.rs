use super::{DynamodbTable, Error, Item};

use aws_sdk_dynamodb::{
    operation::put_item::{builders::PutItemInputBuilder, PutItemInput, PutItemOutput},
    types::{
        AttributeValue, ConditionalOperator, ExpectedAttributeValue, ReturnConsumedCapacity,
        ReturnItemCollectionMetrics, ReturnValue, ReturnValuesOnConditionCheckFailure,
    },
    Client,
};
use std::collections::HashMap;

/// A trait enables your objects to execute DynamoDB PutItem operation.
pub trait PutItem<'a>: DynamodbTable<'a> + Into<Item> {
    fn put_item() -> PutItemOperation<Self> {
        let input_builder = PutItemInput::builder()
            .table_name(Self::TABLE_NAME)
            .set_expected(Self::expected())
            .set_return_values(Self::return_values())
            .set_return_consumed_capacity(Self::return_consumed_capacity())
            .set_return_item_collection_metrics(Self::return_item_collection_metrics())
            .set_conditional_operator(Self::conditional_operator())
            .set_condition_expression(Self::condition_expression())
            .set_expression_attribute_names(Self::expression_attribute_names())
            .set_expression_attribute_values(Self::expression_attribute_values())
            .set_return_values_on_condition_check_failure(
                Self::return_values_on_condition_check_failure(),
            );

        PutItemOperation {
            item: None,
            input_builder,
        }
    }

    /// Return values to be passed as `Expected` to [`PutItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `Expected` option.
    fn expected() -> Option<HashMap<String, ExpectedAttributeValue>> {
        None
    }

    /// Return values to be passed as `ReturnValues` to [`PutItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ReturnValues` option.
    fn return_values() -> Option<ReturnValue> {
        None
    }

    /// Return values to be passed as `ReturnConsumedCapacity` to [`PutItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ReturnConsumedCapacity` option.
    fn return_consumed_capacity() -> Option<ReturnConsumedCapacity> {
        None
    }

    /// Return values to be passed as `ReturnItemCollectionMetrics` to [`PutItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ReturnItemCollectionMetrics` option.
    fn return_item_collection_metrics() -> Option<ReturnItemCollectionMetrics> {
        None
    }

    /// Return values to be passed as `ConditionalOperator` to [`PutItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ConditionalOperator` option.
    fn conditional_operator() -> Option<ConditionalOperator> {
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

    /// Return values to be passed as `ReturnValuesOnConditionCheckFailure` to [`PutItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ReturnValuesOnConditionCheckFailure` option.
    fn return_values_on_condition_check_failure() -> Option<ReturnValuesOnConditionCheckFailure> {
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
