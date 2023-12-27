mod common;

use dynamo_mapper::{
    helpers::{
        attribute_value::AttributeMap,
        expression::update::{self, Update},
    },
    op,
    operations::{
        delete_item::DeleteItem, get_item::GetItem, put_item::PutItem, query::Query,
        update_item::UpdateItem,
    },
    BoxError, DynamodbTable, Item, Key,
};

use aws_sdk_dynamodb::{
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType, ReturnValue,
        ScalarAttributeType,
    },
    Client,
};
use common::{assert_str, assert_u8, get_client, tear_down};

const TABLE_NAME: &str = "E-Commerse";
const PK: &str = "pk";
const SK: &str = "sk";

#[derive(Debug, Clone, PartialEq)]
struct Shop {
    id: String,
    name: String,
}

#[derive(Debug, Clone, PartialEq)]
struct Staff {
    id: String,
    shop_id: String,
    name: String,
    age: u8,
}

#[tokio::test]
async fn get_item() {
    let client = setup().await;

    let shop = Shop {
        id: "1".into(),
        name: "ShoesShop".into(),
    };
    let staff = Staff {
        id: "100".into(),
        shop_id: "1".into(),
        name: "Tanaka".into(),
        age: 20,
    };
    sdk_put_shop(&client, &shop).await;
    sdk_put_staff(&client, &staff).await;

    let result = Shop::get_item()
        .set_key(shop.id.clone(), shop.id.clone())
        .send(&client)
        .await;
    assert!(result.is_ok());

    let opt = result.unwrap();
    assert!(opt.is_some());

    let output = opt.unwrap();
    assert_eq!(output, shop);

    let result = Staff::get_item()
        .set_key(staff.shop_id.clone(), staff.id.clone())
        .send(&client)
        .await;
    assert!(result.is_ok());

    let opt = result.unwrap();
    assert!(opt.is_some());

    let output = opt.unwrap();
    assert_eq!(output, staff);

    tear_down(&client, TABLE_NAME).await;
}

#[tokio::test]
async fn put_item() {
    let client = setup().await;

    let shop = Shop {
        id: "1".into(),
        name: "ShoesShop".into(),
    };
    let staff = Staff {
        id: "100".into(),
        shop_id: "1".into(),
        name: "Tanaka".into(),
        age: 20,
    };

    let result = shop.put().send(&client).await;
    assert!(result.is_ok());

    let result = staff.put().send(&client).await;
    assert!(result.is_ok());

    let opt = sdk_get_item(&client, "SHOP#1", "SHOP#1").await;
    assert!(opt.is_some());

    let item = opt.unwrap();
    assert_str(&item, PK, "SHOP#1");
    assert_str(&item, SK, "SHOP#1");
    assert_str(&item, "id", "1");
    assert_str(&item, "name", "ShoesShop");

    let opt = sdk_get_item(&client, "SHOP#1", "STAFF#100").await;
    assert!(opt.is_some());

    let item = opt.unwrap();
    assert_str(&item, PK, "SHOP#1");
    assert_str(&item, SK, "STAFF#100");
    assert_str(&item, "id", "100");
    assert_str(&item, "shopId", "1");
    assert_str(&item, "name", "Tanaka");
    assert_u8(&item, "age", 20);

    tear_down(&client, TABLE_NAME).await;
}

#[tokio::test]
async fn query() {
    let client = setup().await;

    let staff_1 = Staff {
        id: "100".into(),
        shop_id: "1".into(),
        name: "Tanaka".into(),
        age: 20,
    };
    let staff_2 = Staff {
        id: "200".into(),
        shop_id: "1".into(),
        name: "Suzuki".into(),
        age: 23,
    };
    sdk_put_staff(&client, &staff_1).await;
    sdk_put_staff(&client, &staff_2).await;

    let result = Staff::query().pk_eq("1".into()).send(&client, None).await;
    assert!(result.is_ok());

    let output = result.unwrap();
    assert_eq!(output.items.len(), 2);
    assert!(output.last_evaluated_key.is_none());

    let staff = output.items.get(0).unwrap().clone();
    assert_eq!(staff, staff_1);

    let staff = output.items.get(1).unwrap().clone();
    assert_eq!(staff, staff_2);

    tear_down(&client, TABLE_NAME).await;
}

#[tokio::test]
async fn update_item() {
    let client = setup().await;

    let staff = Staff {
        id: "100".into(),
        shop_id: "1".into(),
        name: "Tanaka".into(),
        age: 10,
    };

    sdk_put_staff(&client, &staff).await;

    let result = staff
        .update()
        .set_update_expression(update::set(op!("#Age").value(op!(":age"))))
        .set_expression_attribute_names([("#Age".to_string(), "age".to_string())].into())
        .set_expression_attribute_values(AttributeMap::new().set_n(":age", "20").into_item())
        .send(&client)
        .await;
    assert!(result.is_ok());

    let opt = result.unwrap();
    assert!(opt.is_some());

    let output = opt.unwrap();
    assert_eq!(output.id, "100");
    assert_eq!(output.shop_id, "1");
    assert_eq!(output.name, "Tanaka");
    assert_eq!(output.age, 20);

    tear_down(&client, TABLE_NAME).await;
}

#[tokio::test]
async fn delete_item() {
    let client = setup().await;

    let staff = Staff {
        id: "100".into(),
        shop_id: "1".into(),
        name: "Tanaka".into(),
        age: 10,
    };

    sdk_put_staff(&client, &staff).await;

    let opt = sdk_get_item(&client, "SHOP#1", "STAFF#100").await;
    assert!(opt.is_some());

    let result = staff.delete().send(&client).await;
    assert!(result.is_ok());

    let opt = result.unwrap();
    assert!(opt.is_none());

    let opt = sdk_get_item(&client, "SHOP#1", "STAFF#100").await;
    assert!(opt.is_none());

    tear_down(&client, TABLE_NAME).await;
}
// -----------------------------------------
// setup section
// -----------------------------------------
impl<'a> DynamodbTable<'a> for Shop {
    const TABLE_NAME: &'a str = TABLE_NAME;

    type Key = ShopKey;

    fn key_inputs(&self) -> (String, String) {
        (self.id.to_string(), self.id.to_string())
    }
}

impl<'a> GetItem<'a> for Shop {}
impl<'a> PutItem<'a> for Shop {}
impl<'a> Query<'a> for Shop {}
impl<'a> UpdateItem<'a> for Shop {
    fn return_values() -> Option<ReturnValue> {
        Some(ReturnValue::AllNew)
    }
}
impl<'a> DeleteItem<'a> for Shop {}

struct ShopKey;

impl<'a> Key<'a> for ShopKey {
    const PARTITION_KEY: &'a str = PK;
    const SORT_KEY: Option<&'a str> = Some(SK);

    type PartitionInput = String;
    type SortInput = String;

    fn partition_key(input: Self::PartitionInput) -> AttributeValue {
        AttributeValue::S(format!("SHOP#{input}"))
    }

    fn sort_key(input: Self::SortInput) -> Option<AttributeValue> {
        Some(AttributeValue::S(format!("SHOP#{input}")))
    }
}

impl TryFrom<Item> for Shop {
    type Error = BoxError;

    fn try_from(item: Item) -> Result<Self, Self::Error> {
        let map = AttributeMap::from(item);
        Ok(Shop {
            id: map.s("id").unwrap().into(),
            name: map.s("name").unwrap().into(),
        })
    }
}

impl From<Shop> for Item {
    fn from(person: Shop) -> Item {
        let Shop { id, name } = person;
        AttributeMap::new()
            .set_s("id", id)
            .set_s("name", name)
            .into_item()
    }
}

impl<'a> DynamodbTable<'a> for Staff {
    const TABLE_NAME: &'a str = TABLE_NAME;

    type Key = StaffKey;

    fn key_inputs(&self) -> (String, String) {
        (self.shop_id.to_string(), self.id.to_string())
    }
}

impl<'a> GetItem<'a> for Staff {}
impl<'a> PutItem<'a> for Staff {}
impl<'a> Query<'a> for Staff {}
impl<'a> UpdateItem<'a> for Staff {
    fn return_values() -> Option<ReturnValue> {
        Some(ReturnValue::AllNew)
    }
}
impl<'a> DeleteItem<'a> for Staff {}

struct StaffKey;

impl<'a> Key<'a> for StaffKey {
    const PARTITION_KEY: &'a str = PK;
    const SORT_KEY: Option<&'a str> = Some(SK);

    type PartitionInput = String;
    type SortInput = String;

    fn partition_key(input: Self::PartitionInput) -> AttributeValue {
        AttributeValue::S(format!("SHOP#{input}"))
    }

    fn sort_key(input: Self::SortInput) -> Option<AttributeValue> {
        Some(AttributeValue::S(format!("STAFF#{input}")))
    }
}

impl TryFrom<Item> for Staff {
    type Error = BoxError;

    fn try_from(item: Item) -> Result<Self, Self::Error> {
        let map = AttributeMap::from(item);
        Ok(Staff {
            id: map.s("id").unwrap().into(),
            shop_id: map.s("shopId").unwrap().into(),
            name: map.s("name").unwrap().into(),
            age: map.n("age").unwrap().parse().unwrap(),
        })
    }
}

impl From<Staff> for Item {
    fn from(person: Staff) -> Item {
        let Staff {
            id,
            shop_id,
            name,
            age,
        } = person;
        AttributeMap::new()
            .set_s("id", id)
            .set_s("shopId", shop_id)
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
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(SK)
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
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(SK)
                .key_type(KeyType::Range)
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

async fn sdk_put_shop(client: &Client, shop: &Shop) {
    let mut item: Item = shop.clone().into();
    item.insert(PK.into(), pk(&shop.id));
    item.insert(SK.into(), sk_shop(&shop.id));

    client
        .put_item()
        .table_name(TABLE_NAME)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
}

async fn sdk_put_staff(client: &Client, staff: &Staff) {
    let mut item: Item = staff.clone().into();
    item.insert(PK.into(), pk(&staff.shop_id));
    item.insert(SK.into(), sk_staff(&staff.id));

    client
        .put_item()
        .table_name(TABLE_NAME)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();
}

async fn sdk_get_item(client: &Client, pk: &str, sk: &str) -> Option<Item> {
    client
        .get_item()
        .table_name(TABLE_NAME)
        .key(PK, AttributeValue::S(pk.into()))
        .key(SK, AttributeValue::S(sk.into()))
        .send()
        .await
        .unwrap()
        .item
}

fn pk(id: &str) -> AttributeValue {
    AttributeValue::S(format!("SHOP#{id}"))
}

fn sk_shop(id: &str) -> AttributeValue {
    AttributeValue::S(format!("SHOP#{id}"))
}

fn sk_staff(id: &str) -> AttributeValue {
    AttributeValue::S(format!("STAFF#{id}"))
}
