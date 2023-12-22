use super::{BoxError, DynamodbTable, Error, Item, KeyBuilder};

use aws_sdk_dynamodb::{
    operation::update_item::{builders::UpdateItemInputBuilder, UpdateItemInput},
    types::{
        AttributeValue, AttributeValueUpdate, ConditionalOperator, ExpectedAttributeValue,
        ReturnConsumedCapacity, ReturnItemCollectionMetrics, ReturnValue,
        ReturnValuesOnConditionCheckFailure,
    },
    Client,
};
use std::collections::HashMap;
use std::marker::PhantomData;

/// A trait enables your objects to execute DynamoDB UpdateItem operation.
pub trait UpdateItem<'a>: DynamodbTable<'a> + TryFrom<Item, Error = BoxError> {
    fn update_item() -> UpdateItemOperation<Self, Self::PkBuilder, Self::SkBuilder> {
        let input_builder = UpdateItemInput::builder()
            .table_name(Self::TABLE_NAME)
            .set_attribute_updates(Self::attribute_updates())
            .set_expected(Self::expected())
            .set_conditional_operator(Self::conditional_operator())
            .set_return_values(Self::return_values())
            .set_return_consumed_capacity(Self::return_consumed_capacity())
            .set_return_item_collection_metrics(Self::return_item_collection_metrics())
            .set_update_expression(Self::update_expression())
            .set_condition_expression(Self::condition_expression())
            .set_expression_attribute_names(Self::expression_attribute_names())
            .set_expression_attribute_values(Self::expression_attribute_values())
            .set_return_values_on_condition_check_failure(
                Self::return_values_on_condition_check_failure(),
            );

        UpdateItemOperation {
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

    /// Return values to be passed as `AttributeUpdates` to [`UpdateItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `AttributeUpdates` option.
    fn attribute_updates() -> Option<HashMap<String, AttributeValueUpdate>> {
        None
    }

    /// Return value to be passed as `Expected` to [`UpdateItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `Expected` option.
    fn expected() -> Option<HashMap<String, ExpectedAttributeValue>> {
        None
    }

    /// Return value to be passed as `ConditionalOperator` to [`UpdateItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ConditionalOperator` option.
    fn conditional_operator() -> Option<ConditionalOperator> {
        None
    }

    /// Return value to be passed as `ReturnValues` to [`UpdateItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ReturnValues` option.
    fn return_values() -> Option<ReturnValue> {
        None
    }

    /// Return value to be passed as `ReturnConsumedCapacity` to [`UpdateItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ReturnConsumedCapacity` option.
    fn return_consumed_capacity() -> Option<ReturnConsumedCapacity> {
        None
    }

    /// Return value to be passed as `ReturnItemCollectionMetrics` to [`UpdateItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ReturnItemCollectionMetrics` option.
    fn return_item_collection_metrics() -> Option<ReturnItemCollectionMetrics> {
        None
    }

    /// Return value to be passed as `UpdateExpression` to [`UpdateItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `UpdateExpression` option.
    fn update_expression() -> Option<String> {
        None
    }

    /// Return value to be passed as `ConditionExpression` to [`ConditionItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ConditionExpression` option.
    fn condition_expression() -> Option<String> {
        None
    }

    /// Return value to be passed as `ExpressionAttributeNames` to [`UpdateItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ExpressionAttributeNames` option.
    fn expression_attribute_names() -> Option<HashMap<String, String>> {
        None
    }

    /// Return value to be passed as `ExpressionAttributeValues` to [`UpdateItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ExpressionAttributeValues` option.
    fn expression_attribute_values() -> Option<Item> {
        None
    }

    /// Return value to be passed as `ReturnValuesOnConditionCheckFailure` to [`UpdateItemInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ReturnValuesOnConditionCheckFailure` option.
    fn return_values_on_condition_check_failure() -> Option<ReturnValuesOnConditionCheckFailure> {
        None
    }
}

/// Represents the DynamoDB UpdateItem operation.
#[derive(Debug, Clone)]
pub struct UpdateItemOperation<T, PkBuilder, SkBuilder>
where
    T: TryFrom<Item, Error = BoxError>,
    PkBuilder: KeyBuilder,
    SkBuilder: KeyBuilder,
{
    pk_attribute: String,
    sk_attribute: Option<String>,
    pk: Option<AttributeValue>,
    sk: Option<AttributeValue>,
    input_builder: UpdateItemInputBuilder,
    item: PhantomData<T>,
    pk_builder: PhantomData<PkBuilder>,
    sk_builder: PhantomData<SkBuilder>,
}

impl<T, PkBuilder, SkBuilder> UpdateItemOperation<T, PkBuilder, SkBuilder>
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

    /// Set update expression
    pub fn update_expression(self, expr: impl Into<String>) -> Self {
        Self {
            input_builder: self.input_builder.update_expression(expr),
            ..self
        }
    }

    /// Set condition expression
    pub fn condition_expression(self, expr: impl Into<String>) -> Self {
        Self {
            input_builder: self.input_builder.condition_expression(expr),
            ..self
        }
    }

    /// Set expression attribute names
    pub fn expression_attribute_names(self, names: HashMap<String, String>) -> Self {
        Self {
            input_builder: self
                .input_builder
                .set_expression_attribute_names(Some(names)),
            ..self
        }
    }

    /// Set expression attribute values
    pub fn expression_attribute_values(self, values: Item) -> Self {
        Self {
            input_builder: self
                .input_builder
                .set_expression_attribute_values(Some(values)),
            ..self
        }
    }

    pub async fn send(self, client: &Client) -> Result<Option<T>, Error> {
        let keys = self.keys();
        let return_value = matches!(
            self.input_builder.get_return_values(),
            Some(ReturnValue::AllNew) | Some(ReturnValue::AllOld)
        );

        let output = self
            .input_builder
            .set_key(Some(keys))
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

    /// Return UpdateItem keys.
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
