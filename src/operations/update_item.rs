use super::{DynamodbTable, Error, Item};

use aws_sdk_dynamodb::{
    operation::update_item::{builders::UpdateItemInputBuilder, UpdateItemInput, UpdateItemOutput},
    types::{
        AttributeValue, ConditionalOperator, ExpectedAttributeValue, ReturnConsumedCapacity,
        ReturnItemCollectionMetrics, ReturnValue, ReturnValuesOnConditionCheckFailure,
    },
    Client,
};
use std::collections::HashMap;

/// Represents the DynamoDB UpdateItem operation.
#[derive(Debug, Clone)]
pub struct UpdateItemOperation<T>
where
    T: Into<Item>,
{
    item: Option<T>,
    input_builder: UpdateItemInputBuilder,
}

impl<T> UpdateItemOperation<T>
where
    T: Into<Item>,
{
    pub fn set_item(self, item: T) -> Self {
        Self {
            item: Some(item),
            ..self
        }
    }

    pub async fn send(self, client: &Client) -> Result<UpdateItemOutput, Error> {
        self.input_builder
            .set_item(self.item.map(|v| v.into()))
            .send_with(client)
            .await
            .map_err(|err| Error::Sdk(Box::new(err)))
    }
}
