use super::{
    helpers::{attribute_value::AttributeMap, condition_expression::begins_with},
    op, BoxError, DynamodbTable, Error, Item, KeyBuilder,
};

use aws_sdk_dynamodb::{
    operation::query::{builders::QueryInputBuilder, QueryInput, QueryOutput},
    types::{AttributeValue, Condition, ConditionalOperator, ReturnConsumedCapacity, Select},
    Client,
};
use std::collections::HashMap;
use std::marker::PhantomData;

/// A trait enables your objects to execute DynamoDB Query operation.
pub trait Query<'a>: DynamodbTable<'a> + TryFrom<Item, Error = BoxError> {
    fn query() -> QueryOperation<Self, Self::PkBuilder, Self::SkBuilder> {
        let input_builder = QueryInput::builder()
            .table_name(Self::TABLE_NAME)
            .set_index_name(Self::index_name())
            .set_select(Self::select())
            .set_attributes_to_get(Self::attribute_to_get())
            .set_limit(Self::limit())
            .set_consistent_read(Self::consistent_read())
            .set_key_conditions(Self::key_conditions())
            .set_query_filter(Self::query_filter())
            .set_conditional_operator(Self::conditional_operator())
            .set_scan_index_forward(Self::scan_index_forward())
            .set_return_consumed_capacity(Self::return_consumed_capacity())
            .set_projection_expression(Self::projection_expression())
            .set_filter_expression(Self::filter_expression())
            .set_expression_attribute_names(Self::expression_attribute_names())
            .set_expression_attribute_values(Self::expression_attribute_values());

        QueryOperation {
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

    /// Return values to be passed as `IndexName` to [`QueryInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `IndexName` option.
    fn index_name() -> Option<String> {
        None
    }

    /// Return value to be passed as `Select` to [`QueryInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `Select` option.
    fn select() -> Option<Select> {
        None
    }

    /// Return values to be passed as `AttributeToGet` to [`QueryInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `AttributeToGet` option.
    fn attribute_to_get() -> Option<Vec<String>> {
        None
    }

    /// Return values to be passed as `Limit` to [`QueryInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `Limit` option.
    fn limit() -> Option<i32> {
        None
    }

    /// Return value to be passed as `ConsistentRead` to [`QueryInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ConsistentRead` option.
    fn consistent_read() -> Option<bool> {
        None
    }

    /// Return value to be passed as `KeyConditions` to [`QueryInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `KeyConditions` option.
    fn key_conditions() -> Option<HashMap<String, Condition>> {
        None
    }

    /// Return value to be passed as `QueryFilter` to [`QueryInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `QueryFilter` option.
    fn query_filter() -> Option<HashMap<String, Condition>> {
        None
    }

    /// Return value to be passed as `ConditionalOperator` to [`QueryInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ConditionalOperator` option.
    fn conditional_operator() -> Option<ConditionalOperator> {
        None
    }

    /// Return value to be passed as `ScanIndexForward` to [`QueryInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ScanIndexForward` option.
    fn scan_index_forward() -> Option<bool> {
        None
    }

    /// Return value to be passed as `ReturnConsumedCapacity` to [`QueryInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ReturnConsumedCapacity` option.
    fn return_consumed_capacity() -> Option<ReturnConsumedCapacity> {
        None
    }

    /// Return value to be passed as `ProjectionExpression` to [`QueryInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ProjectionExpression` option.
    fn projection_expression() -> Option<String> {
        None
    }

    /// Return value to be passed as `FilterExpression` to [`QueryInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `FilterExpression` option.
    fn filter_expression() -> Option<String> {
        None
    }

    /// Return value to be passed as `ExpressionAttributeNames` to [`QueryInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ExpressionAttributeNames` option.
    fn expression_attribute_names() -> Option<HashMap<String, String>> {
        None
    }

    /// Return value to be passed as `ExpressionAttributeValues` to [`QueryInput`].
    /// Default is None.
    ///
    /// You should overwrite this method only if you use `ExpressionAttributeValues` option.
    fn expression_attribute_values() -> Option<HashMap<String, AttributeValue>> {
        None
    }
}

const PK_EXP_NAME: &str = "#PK";
const PK_EXP_VALUE: &str = ":PK";
const SK_EXP_NAME: &str = "#SK";
const SK_EXP_VALUE: &str = ":SK";
const BETWEEN_FROM: &str = ":SK_FROM";
const BETWEEN_TO: &str = ":SK_TO";

#[derive(Debug, Clone, PartialEq)]
enum SkCondition {
    Eq(AttributeValue),
    Lt(AttributeValue),
    Lte(AttributeValue),
    Gt(AttributeValue),
    Gte(AttributeValue),
    Between {
        from: AttributeValue,
        to: AttributeValue,
    },
    BeginsWith(AttributeValue),
}

impl SkCondition {
    fn expression(&self) -> String {
        let sk = op!(SK_EXP_NAME);
        let val = op!(SK_EXP_VALUE);

        let expr = match self {
            Self::Eq(_) => sk.eq(val),
            Self::Lt(_) => sk.lt(val),
            Self::Lte(_) => sk.lte(val),
            Self::Gt(_) => sk.gt(val),
            Self::Gte(_) => sk.gte(val),
            Self::Between { .. } => sk.between(op!(BETWEEN_FROM), op!(BETWEEN_TO)),
            Self::BeginsWith(_) => begins_with(sk, val),
        };

        expr.into()
    }
}

#[derive(Debug, Clone)]
pub struct QueryOperationOutput<T>
where
    T: TryFrom<Item, Error = BoxError>,
{
    pub items: Vec<T>,
    pub last_evaluated_key: Option<Item>,
}

impl<T> TryFrom<QueryOutput> for QueryOperationOutput<T>
where
    T: TryFrom<Item, Error = BoxError>,
{
    type Error = Error;

    fn try_from(output: QueryOutput) -> Result<Self, Self::Error> {
        let mut items: Vec<T> = vec![];

        for item in output.items.unwrap_or_default().into_iter() {
            items.push(item.try_into().map_err(Error::Conversion)?);
        }

        Ok(QueryOperationOutput {
            items,
            last_evaluated_key: output.last_evaluated_key,
        })
    }
}

/// Represents the DynamoDB Query operation.
#[derive(Debug, Clone)]
pub struct QueryOperation<T, PkBuilder, SkBuilder>
where
    T: TryFrom<Item, Error = BoxError>,
    PkBuilder: KeyBuilder,
    SkBuilder: KeyBuilder,
{
    pk_attribute: String,
    sk_attribute: Option<String>,
    pk: Option<AttributeValue>,
    sk: Option<SkCondition>,
    input_builder: QueryInputBuilder,
    item: PhantomData<T>,
    pk_builder: PhantomData<PkBuilder>,
    sk_builder: PhantomData<SkBuilder>,
}

impl<T, PkBuilder, SkBuilder> QueryOperation<T, PkBuilder, SkBuilder>
where
    T: TryFrom<Item, Error = BoxError>,
    PkBuilder: KeyBuilder,
    SkBuilder: KeyBuilder,
{
    /// Set partition key value
    pub fn pk_eq(self, inputs: PkBuilder::Inputs) -> Self {
        Self {
            pk: PkBuilder::build(inputs),
            ..self
        }
    }

    /// Set sort key `equal to` condition
    pub fn sk_eq(self, inputs: SkBuilder::Inputs) -> Self {
        Self {
            sk: SkBuilder::build(inputs).map(SkCondition::Eq),
            ..self
        }
    }

    /// Set sort key `less than` condition
    pub fn sk_lt(self, inputs: SkBuilder::Inputs) -> Self {
        Self {
            sk: SkBuilder::build(inputs).map(SkCondition::Lt),
            ..self
        }
    }

    /// Set sort key `less than or equal to` condition
    pub fn sk_lte(self, inputs: SkBuilder::Inputs) -> Self {
        Self {
            sk: SkBuilder::build(inputs).map(SkCondition::Lte),
            ..self
        }
    }

    /// Set sort key `greater than` condition
    pub fn sk_gt(self, inputs: SkBuilder::Inputs) -> Self {
        Self {
            sk: SkBuilder::build(inputs).map(SkCondition::Gt),
            ..self
        }
    }

    /// Set sort key `greater than or equal to` condition
    pub fn sk_gte(self, inputs: SkBuilder::Inputs) -> Self {
        Self {
            sk: SkBuilder::build(inputs).map(SkCondition::Gte),
            ..self
        }
    }

    /// Set sort key `between A and B` condition
    pub fn sk_between(self, from: SkBuilder::Inputs, to: SkBuilder::Inputs) -> Self {
        let condition = SkBuilder::build(from)
            .zip(SkBuilder::build(to))
            .map(|(from, to)| SkCondition::Between { from, to });
        Self {
            sk: condition,
            ..self
        }
    }

    /// Set sort key `begins_with` condition
    pub fn sk_begins_with(self, value: AttributeValue) -> Self {
        Self {
            sk: Some(SkCondition::BeginsWith(value)),
            ..self
        }
    }

    /// Set `limit` condition
    pub fn limit(self, limit: i32) -> Self {
        Self {
            input_builder: self.input_builder.limit(limit),
            ..self
        }
    }

    /// Set `index name`
    pub fn index(self, name: impl Into<String>) -> Self {
        Self {
            input_builder: self.input_builder.index_name(name),
            ..self
        }
    }

    /// Set `scan index forward` option
    pub fn scan_index_forward(self, forward: bool) -> Self {
        Self {
            input_builder: self.input_builder.scan_index_forward(forward),
            ..self
        }
    }

    /// Set `filter expression`
    ///
    /// **Caution**
    /// You can't use keyword `#PK`, `#SK`, `:PK`, `:SK`, `:SK_FROM` or `:SK_TO` as
    /// ExpressionAttributeNames because these words are used in inner logic of this struct.
    pub fn filter_expression(self, expr: impl Into<String>) -> Self {
        Self {
            input_builder: self.input_builder.filter_expression(expr),
            ..self
        }
    }

    /// Set `expression attribute names` for filter expression.
    ///
    /// **Caution**
    /// You can't use keyword `#PK`, `#SK`, `:PK`, `:SK`, `:SK_FROM` or `:SK_TO` as
    /// ExpressionAttributeNames because these words are used in inner logic of this struct.
    pub fn filter_expression_attribute_names(self, names: HashMap<String, String>) -> Self {
        Self {
            input_builder: self
                .input_builder
                .set_expression_attribute_names(Some(names)),
            ..self
        }
    }

    /// Set `expression attribute values` for filter expression.
    ///
    /// **Caution**
    /// You can't use keyword `#PK`, `#SK`, `:PK`, `:SK`, `:SK_FROM` or `:SK_TO` as
    /// ExpressionAttributeNames because these words are used in inner logic of this struct.
    pub fn filter_expression_attribute_values(self, values: Item) -> Self {
        Self {
            input_builder: self
                .input_builder
                .set_expression_attribute_values(Some(values)),
            ..self
        }
    }

    /// Send Query request with given client and pagination key.
    pub async fn send(
        self,
        client: &Client,
        exclusive_start_key: Option<Item>,
    ) -> Result<QueryOperationOutput<T>, Error> {
        let key_condition_expression = self.key_condition_expression();
        let expression_attribute_names = self.expression_attribute_names();
        let expression_attribute_values = self.expression_attribute_values();

        self.input_builder
            .key_condition_expression(key_condition_expression)
            .set_exclusive_start_key(exclusive_start_key)
            .set_expression_attribute_names(Some(expression_attribute_names))
            .set_expression_attribute_values(Some(expression_attribute_values))
            .send_with(client)
            .await
            .map_err(|err| Error::Sdk(Box::new(err)))
            .and_then(QueryOperationOutput::try_from)
    }

    fn key_condition_expression(&self) -> String {
        let pk_expr = self.pk_condtion_expression();

        if let Some(sk_expr) = self.sk_condition_expression() {
            format!("{pk_expr} AND {sk_expr}")
        } else {
            pk_expr
        }
    }

    fn pk_condtion_expression(&self) -> String {
        op!(PK_EXP_NAME).eq(op!(PK_EXP_VALUE)).into()
    }

    fn sk_condition_expression(&self) -> Option<String> {
        self.sk_attribute
            .as_ref()
            .and(self.sk.as_ref())
            .map(SkCondition::expression)
    }

    fn key_expression_attribute_names(&self) -> HashMap<String, String> {
        let mut names: HashMap<String, String> = HashMap::new();
        names.insert(PK_EXP_NAME.into(), self.pk_attribute.clone());

        if let (Some(sk_attr), Some(_)) = (self.sk_attribute.as_ref(), self.sk.as_ref()) {
            names.insert(SK_EXP_NAME.into(), sk_attr.into());
        }

        names
    }

    fn key_expression_attribute_values(&self) -> Item {
        let mut map = AttributeMap::new().set(
            PK_EXP_VALUE,
            self.pk.clone().expect("Partition key is not set"),
        );

        if self.sk_attribute.is_some() {
            match self.sk.as_ref() {
                Some(
                    SkCondition::Eq(val)
                    | SkCondition::Lt(val)
                    | SkCondition::Lte(val)
                    | SkCondition::Gt(val)
                    | SkCondition::Gte(val)
                    | SkCondition::BeginsWith(val),
                ) => {
                    map = map.set(SK_EXP_VALUE, val.clone());
                }
                Some(SkCondition::Between { from, to }) => {
                    map = map
                        .set(BETWEEN_FROM, from.clone())
                        .set(BETWEEN_TO, to.clone());
                }
                None => {}
            }
        }

        map.into_item()
    }

    fn expression_attribute_names(&self) -> HashMap<String, String> {
        let mut names = self
            .input_builder
            .get_expression_attribute_names()
            .clone()
            .unwrap_or_default();
        names.extend(self.key_expression_attribute_names());
        names
    }

    fn expression_attribute_values(&self) -> Item {
        let mut values = self
            .input_builder
            .get_expression_attribute_values()
            .clone()
            .unwrap_or_default();
        values.extend(self.key_expression_attribute_values());
        values
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_sets_partition_key_condition() {
        let ope = operation().pk_eq("0".to_string());
        assert_eq!(ope.pk, Some(AttributeValue::S("PK#0".to_string())));
    }

    #[test]
    fn it_sets_sort_key_condition_as_equal_to() {
        let ope = operation().sk_eq("1".to_string());
        assert_eq!(
            ope.sk,
            Some(SkCondition::Eq(AttributeValue::S("SK#1".to_string())))
        );
    }

    #[test]
    fn it_sets_sort_key_condition_as_less_than() {
        let ope = operation().sk_lt("1".to_string());
        assert_eq!(
            ope.sk,
            Some(SkCondition::Lt(AttributeValue::S("SK#1".to_string())))
        );
    }

    #[test]
    fn it_sets_sort_key_condition_as_less_than_or_equal_to() {
        let ope = operation().sk_lte("1".to_string());
        assert_eq!(
            ope.sk,
            Some(SkCondition::Lte(AttributeValue::S("SK#1".to_string())))
        );
    }

    #[test]
    fn it_sets_sort_key_condition_as_greater_than() {
        let ope = operation().sk_gt("1".to_string());
        assert_eq!(
            ope.sk,
            Some(SkCondition::Gt(AttributeValue::S("SK#1".to_string())))
        );
    }

    #[test]
    fn it_sets_sort_key_condition_as_greater_than_or_equal_to() {
        let ope = operation().sk_gte("1".to_string());
        assert_eq!(
            ope.sk,
            Some(SkCondition::Gte(AttributeValue::S("SK#1".to_string())))
        );
    }

    #[test]
    fn it_sets_sort_key_condition_as_between_a_and_b() {
        let ope = operation().sk_between("1".to_string(), "2".to_string());
        assert_eq!(
            ope.sk,
            Some(SkCondition::Between {
                from: AttributeValue::S("SK#1".to_string()),
                to: AttributeValue::S("SK#2".to_string()),
            }),
        );
    }

    #[test]
    fn it_sets_limit_condition() {
        let ope = operation().limit(100);
        assert_eq!(ope.input_builder.get_limit().unwrap(), 100,);
    }

    #[test]
    fn it_sets_index_name() {
        let ope = operation().index("test_index");
        assert_eq!(
            ope.input_builder.get_index_name().as_ref().unwrap(),
            "test_index",
        );
    }

    #[test]
    fn it_sets_scan_index_forward_option() {
        let ope = operation().scan_index_forward(false);
        assert_eq!(ope.input_builder.get_scan_index_forward().unwrap(), false,);
    }

    #[test]
    fn it_sets_filter_expression() {
        let ope = operation().filter_expression("#v = :v");
        assert_eq!(
            ope.input_builder.get_filter_expression().as_ref().unwrap(),
            "#v = :v",
        );
    }

    #[test]
    fn it_sets_expression_attribute_names() {
        let mut names: HashMap<String, String> = HashMap::new();
        names.insert("#v".into(), "foo".into());

        let ope = operation().filter_expression_attribute_names(names);
        let map = ope
            .input_builder
            .get_expression_attribute_names()
            .as_ref()
            .unwrap();
        assert_eq!(map.get("#v").unwrap(), &"foo".to_string());
    }

    #[test]
    fn it_sets_expression_attribute_values() {
        let item = AttributeMap::new().set_s(":v", "bar").into_item();

        let ope = operation().filter_expression_attribute_values(item);
        let map = ope
            .input_builder
            .get_expression_attribute_values()
            .as_ref()
            .unwrap();
        assert_eq!(map.get(":v").unwrap(), &AttributeValue::S("bar".into()));
    }

    #[test]
    fn it_creates_partition_key_condition_expression() {
        let ope = operation();
        assert_eq!(ope.pk_condtion_expression(), "#PK = :PK");
    }

    #[test]
    fn it_creates_condition_expression_names() {
        let mut names: HashMap<String, String> = HashMap::new();
        names.insert("#v".into(), "foo".into());

        let ope = operation().filter_expression_attribute_names(names);
        let names = ope.expression_attribute_names();
        assert_eq!(names.get("#PK").unwrap(), &"partition key".to_string());
        assert_eq!(names.get("#v").unwrap(), &"foo".to_string());
    }

    #[test]
    fn it_creates_condition_expression_values() {
        let item = AttributeMap::new().set_s(":v", "bar").into_item();

        let ope = operation()
            .pk_eq("0".into())
            .filter_expression_attribute_values(item);
        let values = ope.expression_attribute_values();
        assert_eq!(
            values.get(":PK").unwrap(),
            &AttributeValue::S("PK#0".into())
        );
        assert_eq!(values.get(":v").unwrap(), &AttributeValue::S("bar".into()));
    }

    mod partition_key_only {
        use super::*;

        #[test]
        fn it_creates_sort_key_condition_expression() {
            let ope = operation();
            assert!(ope.sk_condition_expression().is_none());

            // Even if any sort key condition is set, it returns None.
            let ope = operation().sk_eq("1".into());
            assert!(ope.sk_condition_expression().is_none());
        }

        #[test]
        fn it_creates_key_condition_expression() {
            // Even if any sort key condition is set, it returns partition key condition.
            let ope = operation().pk_eq("0".into()).sk_eq("1".into());
            assert_eq!(ope.key_condition_expression(), "#PK = :PK");
        }

        #[test]
        fn it_creates_key_condition_expression_names() {
            // Even if any sort key condition is set, it returns partition key values.
            let ope = operation();
            let names = ope.key_expression_attribute_names();
            assert_eq!(names.get("#PK").unwrap(), &"partition key".to_string());
            assert!(names.get("#SK").is_none());
        }

        #[test]
        fn it_creates_key_condition_expression_values() {
            // Even if any sort key condition is set, it returns partition key values.
            let ope = operation().pk_eq("0".into()).sk_eq("1".into());
            let values = ope.key_expression_attribute_values();
            assert_eq!(
                values.get(":PK").unwrap(),
                &AttributeValue::S("PK#0".into())
            );
            assert!(values.get(":SK").is_none());
        }
    }

    mod sort_key_exists {
        use super::*;

        #[test]
        fn it_creates_sort_key_condition_expression() {
            // When any value is not set, it returns None.
            let ope = operation_with_sk();
            assert!(ope.sk_condition_expression().is_none());

            // When any value is set, it returns the value.
            let ope = operation_with_sk().sk_eq("1".into());
            assert_eq!(ope.sk_condition_expression(), Some("#SK = :SK".into()));
        }

        #[test]
        fn it_creates_key_condition_expression() {
            // When sort key condition is not set, it returns condition expression for partition key
            // only.
            let ope = operation_with_sk().pk_eq("0".into());
            assert_eq!(ope.key_condition_expression(), "#PK = :PK");

            // When any value is set, it returns the value.
            let ope = operation_with_sk().pk_eq("0".into()).sk_eq("1".into());
            assert_eq!(ope.key_condition_expression(), "#PK = :PK AND #SK = :SK");
        }

        #[test]
        fn it_creates_key_condition_expression_names() {
            // When sort key condition is not set, it returns condition expression for partition key
            // only.
            let ope = operation_with_sk();
            let names = ope.key_expression_attribute_names();
            assert_eq!(names.get("#PK").unwrap(), &"partition key".to_string());
            assert!(names.get("#SK").is_none());

            // When any value is set, it returns condition expression for partition key and sort
            // key.
            let ope = operation_with_sk().sk_eq("1".into());
            let names = ope.key_expression_attribute_names();
            assert_eq!(names.get("#PK").unwrap(), &"partition key".to_string());
            assert_eq!(names.get("#SK").unwrap(), &"sort key".to_string());
        }

        #[test]
        fn it_creates_key_condition_expression_values() {
            // Even if any sort key condition is not set, it returns partition key values.
            let ope = operation_with_sk().pk_eq("0".into());
            let values = ope.key_expression_attribute_values();
            assert_eq!(
                values.get(":PK").unwrap(),
                &AttributeValue::S("PK#0".into())
            );
            assert!(values.get(":SK").is_none());

            // If any sort key condition is set, it returns partition key and sort key values.
            let ope = operation_with_sk().pk_eq("0".into()).sk_eq("1".into());
            let values = ope.key_expression_attribute_values();
            assert_eq!(
                values.get(":PK").unwrap(),
                &AttributeValue::S("PK#0".into())
            );
            assert_eq!(
                values.get(":SK").unwrap(),
                &AttributeValue::S("SK#1".into())
            );

            // If any sort key condition is set as between A and B, it returns partition key
            // and sort key values.
            let ope = operation_with_sk()
                .pk_eq("0".into())
                .sk_between("1".into(), "2".into());
            let values = ope.key_expression_attribute_values();
            assert_eq!(
                values.get(":PK").unwrap(),
                &AttributeValue::S("PK#0".into())
            );
            assert_eq!(
                values.get(":SK_FROM").unwrap(),
                &AttributeValue::S("SK#1".into())
            );
            assert_eq!(
                values.get(":SK_TO").unwrap(),
                &AttributeValue::S("SK#2".into())
            );
        }
    }

    fn operation() -> QueryOperation<Person, PartitionKey, SortKey> {
        QueryOperation {
            pk_attribute: "partition key".to_string(),
            sk_attribute: None,
            pk: None,
            sk: None,
            input_builder: QueryInput::builder(),
            item: PhantomData,
            pk_builder: PhantomData,
            sk_builder: PhantomData,
        }
    }

    fn operation_with_sk() -> QueryOperation<Person, PartitionKey, SortKey> {
        QueryOperation {
            pk_attribute: "partition key".to_string(),
            sk_attribute: Some("sort key".into()),
            pk: None,
            sk: None,
            input_builder: QueryInput::builder(),
            item: PhantomData,
            pk_builder: PhantomData,
            sk_builder: PhantomData,
        }
    }

    struct Person;

    impl TryFrom<Item> for Person {
        type Error = BoxError;

        fn try_from(_item: Item) -> Result<Self, Self::Error> {
            Ok(Person {})
        }
    }

    struct PartitionKey;

    impl KeyBuilder for PartitionKey {
        type Inputs = String;
        fn build(inputs: Self::Inputs) -> Option<AttributeValue> {
            Some(AttributeValue::S(format!("PK#{inputs}")))
        }
    }

    struct SortKey;

    impl KeyBuilder for SortKey {
        type Inputs = String;
        fn build(inputs: Self::Inputs) -> Option<AttributeValue> {
            Some(AttributeValue::S(format!("SK#{inputs}")))
        }
    }
}
