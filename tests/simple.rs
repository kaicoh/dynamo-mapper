mod common;

use dynamo_mapper::{
    operations::{get_item::GetItem, put_item::PutItem},
    BoxError, DynamodbTable, Item, KeyBuilder, NotKey,
};

use aws_sdk_dynamodb::{
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    },
    Client,
};
use common::{get_client, get_str, get_u8, tear_down};
use std::collections::HashMap;

const TABLE_NAME: &str = "People";
const PK: &str = "pk";

#[derive(Debug, PartialEq)]
struct Person {
    id: String,
    name: String,
    age: u8,
}

#[tokio::test]
async fn get_item() {
    let client = get_client();
    create_table(&client).await;

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
    let client = get_client();
    create_table(&client).await;

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
    let client = get_client();
    create_table(&client).await;

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
        Ok(Person {
            id: get_str(&item, "id"),
            name: get_str(&item, "name"),
            age: get_u8(&item, "age"),
        })
    }
}

impl From<Person> for Item {
    fn from(person: Person) -> Item {
        let Person { id, name, age } = person;
        let mut item: Item = HashMap::new();

        item.insert(PK.into(), pk(&id));
        item.insert("id".into(), AttributeValue::S(id));
        item.insert("name".into(), AttributeValue::S(name));
        item.insert("age".into(), AttributeValue::N(age.to_string()));

        item
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

async fn sdk_put_item(client: &Client, person: &Person) {
    let mut item: Item = HashMap::new();

    item.insert(PK.into(), pk(&person.id));
    item.insert("id".into(), AttributeValue::S(person.id.to_string()));
    item.insert("name".into(), AttributeValue::S(person.name.to_string()));
    item.insert("age".into(), AttributeValue::N(person.age.to_string()));

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

fn assert_str(item: &Item, key: &str, expected: &str) {
    match item.get(key).as_ref() {
        Some(&AttributeValue::S(val)) => {
            assert_eq!(val.as_str(), expected);
        }
        _ => {
            unreachable!("{key} value is not what is expected");
        }
    }
}

fn assert_u8(item: &Item, key: &str, expected: u8) {
    match item.get(key).as_ref() {
        Some(&AttributeValue::N(val)) => {
            let actual: u8 = val.parse().expect("{key} value must be a `u8`");
            assert_eq!(actual, expected);
        }
        _ => {
            unreachable!("{key} value is not what is expected");
        }
    }
}