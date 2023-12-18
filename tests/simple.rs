mod common;

use dynamo_mapper::{
    operations::get_item::GetItem, BoxError, DynamodbTable, Item, KeyBuilder, NotKey,
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
    put_item(&client, &person).await;

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
async fn put_item(client: &Client, person: &Person) {
    let mut item: Item = HashMap::new();

    item.insert(
        PK.into(),
        AttributeValue::S(format!("PERSON#{}", person.id)),
    );
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
