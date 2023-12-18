use super::Item;

use aws_sdk_dynamodb::{primitives::Blob, types::AttributeValue};
use std::collections::HashMap;

/// A wrapper for HashMap<String, AttributeValue>.
///
/// You can build HashMap<String, AttributeValue> as if using builder pattern.
/// ```
/// # use aws_sdk_dynamodb::types::AttributeValue;
/// # use dynamo_mapper::helpers::attribute_value::AttributeMap;
/// # use std::collections::HashMap;
/// let item = AttributeMap::new()
///     .set_n("id", "123")
///     .set_s("name", "tanaka")
///     .into_item();
///
/// let mut expected: HashMap<String, AttributeValue> = HashMap::new();
/// expected.insert("id".into(), AttributeValue::N("123".into()));
/// expected.insert("name".into(), AttributeValue::S("tanaka".into()));
///
/// assert_eq!(item, expected);
/// ```
///
/// And you can extract inner values easily.
/// ```
/// # use aws_sdk_dynamodb::types::AttributeValue;
/// # use dynamo_mapper::helpers::attribute_value::AttributeMap;
/// # use std::collections::HashMap;
/// let item = AttributeMap::new()
///     .set_n("id", "123")
///     .set_s("name", "tanaka");
///
/// assert_eq!(item.n("id"), Some(&String::from("123")));
/// assert_eq!(item.s("name"), Some(&String::from("tanaka")));
/// ```
#[derive(Debug, Clone)]
pub struct AttributeMap(Item);

impl AttributeMap {
    /// Create a new instance.
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Get AttributeValue from the key.
    pub fn get(&self, key: &str) -> Option<&AttributeValue> {
        self.0.get(key)
    }

    /// Get inner value of AttributeValue::B from the key.
    pub fn b(&self, key: &str) -> Option<&Blob> {
        self.get(key).and_then(opt_b)
    }

    /// Get inner value of AttributeValue::Bool from the key.
    pub fn bool(&self, key: &str) -> Option<&bool> {
        self.get(key).and_then(opt_bool)
    }

    /// Get inner value of AttributeValue::Bs from the key.
    pub fn bs(&self, key: &str) -> Option<&Vec<Blob>> {
        self.get(key).and_then(opt_bs)
    }

    /// Get inner value of AttributeValue::L from the key.
    pub fn l(&self, key: &str) -> Option<&Vec<AttributeValue>> {
        self.get(key).and_then(opt_l)
    }

    /// Get inner value of AttributeValue::M from the key.
    pub fn m(&self, key: &str) -> Option<&Item> {
        self.get(key).and_then(opt_m)
    }

    /// Get inner value of AttributeValue::N from the key.
    pub fn n(&self, key: &str) -> Option<&String> {
        self.get(key).and_then(opt_n)
    }

    /// Get inner value of AttributeValue::Ns from the key.
    pub fn ns(&self, key: &str) -> Option<&Vec<String>> {
        self.get(key).and_then(opt_ns)
    }

    /// Get inner value of AttributeValue::Null from the key.
    pub fn null(&self, key: &str) -> Option<&bool> {
        self.get(key).and_then(opt_null)
    }

    /// Get inner value of AttributeValue::S from the key.
    pub fn s(&self, key: &str) -> Option<&String> {
        self.get(key).and_then(opt_s)
    }

    /// Get inner value of AttributeValue::Ss from the key.
    pub fn ss(&self, key: &str) -> Option<&Vec<String>> {
        self.get(key).and_then(opt_ss)
    }

    /// Set AttributeValue with the key.
    pub fn set(mut self, key: impl Into<String>, val: AttributeValue) -> Self {
        self.0.insert(key.into(), val);
        self
    }

    /// Set the value as AttributeValue::B with the key.
    pub fn set_b(self, key: impl Into<String>, val: impl Into<Blob>) -> Self {
        self.set(key, AttributeValue::B(val.into()))
    }

    /// Set the value as AttributeValue::Bool with the key.
    pub fn set_bool(self, key: impl Into<String>, val: bool) -> Self {
        self.set(key, AttributeValue::Bool(val))
    }

    /// Set the value as AttributeValue::Bs with the key.
    pub fn set_bs<T, B>(self, key: impl Into<String>, val: T) -> Self
    where
        T: IntoIterator<Item = B>,
        B: Into<Blob>,
    {
        let val: Vec<Blob> = val.into_iter().map(|v| v.into()).collect();
        self.set(key, AttributeValue::Bs(val))
    }

    /// Set the value as AttributeValue::L with the key.
    pub fn set_l<T, A>(self, key: impl Into<String>, val: T) -> Self
    where
        T: IntoIterator<Item = A>,
        A: Into<AttributeValue>,
    {
        let val: Vec<AttributeValue> = val.into_iter().map(|v| v.into()).collect();
        self.set(key, AttributeValue::L(val))
    }

    /// Set the value as AttributeValue::M with the key.
    pub fn set_m(self, key: impl Into<String>, val: impl Into<Item>) -> Self {
        self.set(key, AttributeValue::M(val.into()))
    }

    /// Set the value as AttributeValue::N with the key.
    pub fn set_n(self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.set(key, AttributeValue::N(val.into()))
    }

    /// Set the value as AttributeValue::Ns with the key.
    pub fn set_ns<T, S>(self, key: impl Into<String>, val: T) -> Self
    where
        T: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let val: Vec<String> = val.into_iter().map(|v| v.into()).collect();
        self.set(key, AttributeValue::Ns(val))
    }

    /// Set the value as AttributeValue::Null with the key.
    pub fn set_null(self, key: impl Into<String>, val: bool) -> Self {
        self.set(key, AttributeValue::Null(val))
    }

    /// Set the value as AttributeValue::S with the key.
    pub fn set_s(self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.set(key, AttributeValue::S(val.into()))
    }

    /// Set the value as AttributeValue::Ss with the key.
    pub fn set_ss<T, S>(self, key: impl Into<String>, val: T) -> Self
    where
        T: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let val: Vec<String> = val.into_iter().map(|v| v.into()).collect();
        self.set(key, AttributeValue::Ss(val))
    }

    /// Convert self into HashMap<String, AttributeValue>.
    pub fn into_item(self) -> Item {
        self.into()
    }

    /// Convert self into AttributeValue::M.
    pub fn into_m(self) -> AttributeValue {
        AttributeValue::M(self.into())
    }
}

impl Default for AttributeMap {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Item> for AttributeMap {
    fn from(item: Item) -> Self {
        Self(item)
    }
}

impl From<AttributeMap> for Item {
    fn from(item: AttributeMap) -> Self {
        item.0
    }
}

/// Get inner value from the AttributeValue::B.
pub fn opt_b(val: &AttributeValue) -> Option<&Blob> {
    val.as_b().ok()
}

/// Get inner value from the AttributeValue::Bool.
pub fn opt_bool(val: &AttributeValue) -> Option<&bool> {
    val.as_bool().ok()
}

/// Get inner value from the AttributeValue::Bs.
pub fn opt_bs(val: &AttributeValue) -> Option<&Vec<Blob>> {
    val.as_bs().ok()
}

/// Get inner value from the AttributeValue::L.
pub fn opt_l(val: &AttributeValue) -> Option<&Vec<AttributeValue>> {
    val.as_l().ok()
}

/// Get inner value from the AttributeValue::M.
pub fn opt_m(val: &AttributeValue) -> Option<&Item> {
    val.as_m().ok()
}

/// Get inner value from the AttributeValue::N.
pub fn opt_n(val: &AttributeValue) -> Option<&String> {
    val.as_n().ok()
}

/// Get inner value from the AttributeValue::Ns.
pub fn opt_ns(val: &AttributeValue) -> Option<&Vec<String>> {
    val.as_ns().ok()
}

/// Get inner value from the AttributeValue::Null.
pub fn opt_null(val: &AttributeValue) -> Option<&bool> {
    val.as_null().ok()
}

/// Get inner value from the AttributeValue::S.
pub fn opt_s(val: &AttributeValue) -> Option<&String> {
    val.as_s().ok()
}

/// Get inner value from the AttributeValue::Ss.
pub fn opt_ss(val: &AttributeValue) -> Option<&Vec<String>> {
    val.as_ss().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opt_b_returns_ref_of_blob() {
        let value = AttributeValue::B(Blob::new("hello"));
        assert_eq!(opt_b(&value), Some(&Blob::new("hello")));

        let value = AttributeValue::S("hello".into());
        assert!(opt_b(&value).is_none());
    }

    #[test]
    fn opt_bool_returns_ref_of_bool() {
        let value = AttributeValue::Bool(true);
        assert_eq!(opt_bool(&value), Some(&true));

        let value = AttributeValue::S("hello".into());
        assert!(opt_bool(&value).is_none());
    }

    #[test]
    fn opt_bs_returns_ref_of_blobs() {
        let value = AttributeValue::Bs(vec![Blob::new("hello")]);
        assert_eq!(opt_bs(&value), Some(&vec![Blob::new("hello")]));

        let value = AttributeValue::S("hello".into());
        assert!(opt_bs(&value).is_none());
    }

    #[test]
    fn opt_l_returns_ref_of_attribute_values() {
        let value = AttributeValue::L(vec![AttributeValue::N("100".into())]);
        assert_eq!(opt_l(&value), Some(&vec![AttributeValue::N("100".into())]));

        let value = AttributeValue::S("hello".into());
        assert!(opt_l(&value).is_none());
    }

    #[test]
    fn opt_m_returns_ref_of_hash_map_from_attribute_values() {
        let item: Item = [
            ("name".to_string(), AttributeValue::S("Tanaka".into())),
            ("age".to_string(), AttributeValue::N("20".into())),
        ]
        .into();

        let value = AttributeValue::M(item.clone());
        assert_eq!(opt_m(&value), Some(&item));

        let value = AttributeValue::S("hello".into());
        assert!(opt_m(&value).is_none());
    }

    #[test]
    fn opt_n_returns_ref_of_string() {
        let value = AttributeValue::N("100".into());
        assert_eq!(opt_n(&value), Some(&"100".into()));

        let value = AttributeValue::S("hello".into());
        assert!(opt_n(&value).is_none());
    }

    #[test]
    fn opt_ns_returns_ref_of_strings() {
        let value = AttributeValue::Ns(vec!["100".into()]);
        assert_eq!(opt_ns(&value), Some(&vec!["100".into()]));

        let value = AttributeValue::S("hello".into());
        assert!(opt_ns(&value).is_none());
    }

    #[test]
    fn opt_null_returns_ref_of_bool() {
        let value = AttributeValue::Null(true);
        assert_eq!(opt_null(&value), Some(&true));

        let value = AttributeValue::S("hello".into());
        assert!(opt_null(&value).is_none());
    }

    #[test]
    fn opt_s_returns_ref_of_string() {
        let value = AttributeValue::S("hello".into());
        assert_eq!(opt_s(&value), Some(&"hello".into()));

        let value = AttributeValue::N("100".into());
        assert!(opt_s(&value).is_none());
    }

    #[test]
    fn opt_ss_returns_ref_of_strings() {
        let value = AttributeValue::Ss(vec!["hello".into(), "world".into()]);
        assert_eq!(opt_ss(&value), Some(&vec!["hello".into(), "world".into()]));

        let value = AttributeValue::S("hello".into());
        assert!(opt_ss(&value).is_none());
    }
}
