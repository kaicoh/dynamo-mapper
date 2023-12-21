mod common;

use dynamo_mapper::{
    helpers::attribute_value::AttributeMap,
    operations::{get_item::GetItem, put_item::PutItem, query::Query},
    BoxError, DynamodbTable, Item, KeyBuilder, NotKey,
};

use aws_sdk_dynamodb::{
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    },
    Client,
};
use common::{assert_str, assert_u8, get_client, tear_down};

const TABLE_NAME: &str = "People";
const PK: &str = "pk";

#[derive(Debug, Clone, PartialEq)]
struct Person {
    id: String,
    name: String,
    age: u8,
}

#[tokio::test]
async fn get_item() {
    let client = setup().await;

    let person = Person {
        id: "12345".into(),
        name: "Tanaka".into(),
        age: 20,
    };
    sdk_put_item(&client, &person).await;

    let result = Person::get_item()
        .set_pk(person.id.clone())
        .send(&client)
        .await;
    assert!(result.is_ok());

    let opt = result.unwrap();
    assert!(opt.is_some());

    let output = opt.unwrap();
    assert_eq!(output, person);

    tear_down(&client, TABLE_NAME).await;
}

#[tokio::test]
async fn get_item_but_not_found() {
    let client = setup().await;

    let result = Person::get_item()
        .set_pk("not found".into())
        .send(&client)
        .await;
    assert!(result.is_ok());

    let opt = result.unwrap();
    assert!(opt.is_none());

    tear_down(&client, TABLE_NAME).await;
}

#[tokio::test]
async fn put_item() {
    let client = setup().await;

    let person = Person {
        id: "12345".into(),
        name: "Tanaka".into(),
        age: 20,
    };

    let result = Person::put_item().set_item(person).send(&client).await;
    assert!(result.is_ok());

    let opt = sdk_get_item(&client, "PERSON#12345").await;
    assert!(opt.is_some());

    let item = opt.unwrap();
    assert_str(&item, PK, "PERSON#12345");
    assert_str(&item, "id", "12345");
    assert_str(&item, "name", "Tanaka");
    assert_u8(&item, "age", 20);

    tear_down(&client, TABLE_NAME).await;
}

#[tokio::test]
async fn query() {
    let client = setup().await;

    let person_0 = Person {
        id: "0".into(),
        name: "Tanaka".into(),
        age: 10,
    };
    let person_1 = Person {
        id: "1".into(),
        name: "Suzuki".into(),
        age: 20,
    };

    sdk_put_item(&client, &person_0).await;
    sdk_put_item(&client, &person_1).await;

    let result = Person::query().pk_eq("0".into()).send(&client, None).await;
    assert!(result.is_ok());

    let output = result.unwrap();
    assert_eq!(output.items.len(), 1);
    assert!(output.last_evaluated_key.is_none());

    let person = output.items.get(0).unwrap().clone();
    assert_eq!(person, person_0);

    tear_down(&client, TABLE_NAME).await;
}

// -----------------------------------------
// setup section
// -----------------------------------------
impl<'a> DynamodbTable<'a> for Person {
    const TABLE_NAME: &'a str = TABLE_NAME;
    const PK_ATTRIBUTE: &'a str = PK;
    const SK_ATTRIBUTE: Option<&'a str> = None;

    type PkBuilder = PkBuilder;
    type SkBuilder = NotKey;
}

impl<'a> GetItem<'a> for Person {}
impl<'a> PutItem<'a> for Person {}
impl<'a> Query<'a> for Person {}

struct PkBuilder;

impl KeyBuilder for PkBuilder {
    type Inputs = String;

    fn build(inputs: Self::Inputs) -> Option<AttributeValue> {
        Some(AttributeValue::S(format!("PERSON#{}", inputs)))
    }
}

impl TryFrom<Item> for Person {
    type Error = BoxError;

    fn try_from(item: Item) -> Result<Self, Self::Error> {
        let map = AttributeMap::from(item);
        Ok(Person {
            id: map.s("id").unwrap().into(),
            name: map.s("name").unwrap().into(),
            age: map.n("age").unwrap().parse().unwrap(),
        })
    }
}

impl From<Person> for Item {
    fn from(person: Person) -> Item {
        let Person { id, name, age } = person;
        AttributeMap::new()
            .set(PK, pk(&id))
            .set_s("id", id)
            .set_s("name", name)
            .set_n("age", age.to_string())
            .into_item()
    }
}

// -----------------------------------------
// utility section
// -----------------------------------------
async fn create_table(client: &Client) {
    client
        .create_table()
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(PK)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .table_name(TABLE_NAME)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(PK)
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
}

async fn setup() -> Client {
    let client = get_client();
    create_table(&client).await;
    client
}

async fn sdk_put_item(client: &Client, person: &Person) {
    let Person { id, name, age } = person;
    let item = AttributeMap::new()
        .set(PK, pk(id))
        .set_s("id", id)
        .set_s("name", name)
        .set_n("age", age.to_string())
        .into_item();

    client
        .put_item()
        .table_name(TABLE_NAME)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
}

async fn sdk_get_item(client: &Client, pk: &str) -> Option<Item> {
    client
        .get_item()
        .table_name(TABLE_NAME)
        .key(PK, AttributeValue::S(pk.into()))
        .send()
        .await
        .unwrap()
        .item
}

fn pk(id: &str) -> AttributeValue {
    AttributeValue::S(format!("PERSON#{id}"))
}
