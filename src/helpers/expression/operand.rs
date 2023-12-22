use std::fmt;

/// An operand of which the condition expressions consist.
#[derive(Debug, Clone, PartialEq)]
pub struct Operand(String);

impl fmt::Display for Operand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Operand {
    /// A constructor of Operand. You can also use `op!` macro to create an Operand.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// # use dynamo_mapper::helpers::expression::Operand;
    /// let op_0 = Operand::new("#foo");
    /// let op_1 = op!("#foo");
    /// assert_eq!(op_0, op_1);
    /// ```
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}
