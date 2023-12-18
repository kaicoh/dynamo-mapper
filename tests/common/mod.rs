use dynamo_mapper::Item;

use aws_config::{retry::RetryConfig, BehaviorVersion, Region, SdkConfig};
use aws_credential_types::{provider::SharedCredentialsProvider, Credentials};
use aws_sdk_dynamodb::Client;
use ulid::Ulid;

pub fn get_client() -> Client {
    let creds = Credentials::from_keys(Ulid::new(), Ulid::new(), None);
    let creds_provider = SharedCredentialsProvider::new(creds);

    let retry = RetryConfig::standard().with_max_attempts(5);

    let config = SdkConfig::builder()
        .endpoint_url("http://localhost:8000")
        .credentials_provider(creds_provider)
        .retry_config(retry)
        .behavior_version(BehaviorVersion::latest())
        .region(Some(Region::from_static("us-east-1")))
        .build();

    Client::new(&config)
}

pub async fn tear_down(client: &Client, table_name: &str) {
    drop_table(client, table_name).await;
}

async fn drop_table(client: &Client, table_name: &str) {
    client
        .delete_table()
        .table_name(table_name)
        .send()
        .await
        .unwrap();
}

pub fn get_str(item: &Item, key: &str) -> String {
    item.get(key)
        .and_then(|v| v.as_s().ok())
        .map(|v| v.to_string())
        .unwrap_or_default()
}

pub fn get_u8(item: &Item, key: &str) -> u8 {
    item.get(key)
        .and_then(|v| v.as_n().ok())
        .and_then(|v| v.parse().ok())
        .unwrap()
}
