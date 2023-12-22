use std::fmt;

use super::Operand;

impl Condition for Operand {}

pub trait Condition: Into<Operand> {
    /// Create an `equal to` condition expression.
    ///
    /// ```
    /// use dynamo_mapper::op;
    /// use dynamo_mapper::helpers::expression::condition::Condition;
    ///
    /// let expr = op!("#x").equal(op!(":x"));
    /// assert_eq!(expr.to_string(), "#x = :x");
    /// ```
    fn equal(self, operand: impl Into<Operand>) -> ConditionExpression {
        ConditionExpression::Compare {
            left: self.into(),
            right: operand.into(),
            comperator: Comperator::Eq,
        }
    }

    /// Create a `not equal to` condition expression.
    ///
    /// ```
    /// use dynamo_mapper::op;
    /// use dynamo_mapper::helpers::expression::condition::Condition;
    ///
    /// let expr = op!("#x").ne(op!(":x"));
    /// assert_eq!(expr.to_string(), "#x <> :x");
    /// ```
    fn ne(self, operand: impl Into<Operand>) -> ConditionExpression {
        ConditionExpression::Compare {
            left: self.into(),
            right: operand.into(),
            comperator: Comperator::Ne,
        }
    }

    /// Create a `less than` condition expression.
    ///
    /// ```
    /// use dynamo_mapper::op;
    /// use dynamo_mapper::helpers::expression::condition::Condition;
    ///
    /// let expr = op!("#x").lt(op!(":x"));
    /// assert_eq!(expr.to_string(), "#x < :x");
    fn lt(self, operand: impl Into<Operand>) -> ConditionExpression {
        ConditionExpression::Compare {
            left: self.into(),
            right: operand.into(),
            comperator: Comperator::Lt,
        }
    }

    /// Create a `less than or equal to` condition expression.
    ///
    /// ```
    /// use dynamo_mapper::op;
    /// use dynamo_mapper::helpers::expression::condition::Condition;
    ///
    /// let expr = op!("#x").lte(op!(":x"));
    /// assert_eq!(expr.to_string(), "#x <= :x");
    fn lte(self, operand: impl Into<Operand>) -> ConditionExpression {
        ConditionExpression::Compare {
            left: self.into(),
            right: operand.into(),
            comperator: Comperator::Lte,
        }
    }

    /// Create a `greater than` condition expression.
    ///
    /// ```
    /// use dynamo_mapper::op;
    /// use dynamo_mapper::helpers::expression::condition::Condition;
    ///
    /// let expr = op!("#x").gt(op!(":x"));
    /// assert_eq!(expr.to_string(), "#x > :x");
    /// ```
    fn gt(self, operand: impl Into<Operand>) -> ConditionExpression {
        ConditionExpression::Compare {
            left: self.into(),
            right: operand.into(),
            comperator: Comperator::Gt,
        }
    }

    /// Create a `greater than or equal to` condition expression.
    ///
    /// ```
    /// use dynamo_mapper::op;
    /// use dynamo_mapper::helpers::expression::condition::Condition;
    ///
    /// let expr = op!("#x").gte(op!(":x"));
    /// assert_eq!(expr.to_string(), "#x >= :x");
    /// ```
    fn gte(self, operand: impl Into<Operand>) -> ConditionExpression {
        ConditionExpression::Compare {
            left: self.into(),
            right: operand.into(),
            comperator: Comperator::Gte,
        }
    }

    /// Create a `between A and B` condition expression.
    ///
    /// ```
    /// use dynamo_mapper::op;
    /// use dynamo_mapper::helpers::expression::condition::Condition;
    ///
    /// let expr = op!("#x").between(op!(":x"), op!(":y"));
    /// assert_eq!(expr.to_string(), "#x BETWEEN :x AND :y");
    /// ```
    fn between(self, from: impl Into<Operand>, to: impl Into<Operand>) -> ConditionExpression {
        ConditionExpression::Between {
            operand: self.into(),
            from: from.into(),
            to: to.into(),
        }
    }

    /// Create an `in any of the values` condition expression.
    ///
    /// ```
    /// use dynamo_mapper::op;
    /// use dynamo_mapper::helpers::expression::condition::Condition;
    ///
    /// let expr = op!("#x").any([op!(":x"), op!(":y")]);
    /// assert_eq!(expr.to_string(), "#x IN (:x, :y)");
    /// ```
    fn any(self, values: impl IntoIterator<Item = Operand>) -> ConditionExpression {
        ConditionExpression::Any {
            operand: self.into(),
            values: values.into_iter().collect(),
        }
    }
}

/// ConditionExpression
#[derive(Debug, Clone, PartialEq)]
pub enum ConditionExpression {
    /// condition expression with comperator (=, <>, <, <=, >, >=).
    Compare {
        left: Operand,
        right: Operand,
        comperator: Comperator,
    },

    /// `between A and B` condition.
    Between {
        operand: Operand,
        from: Operand,
        to: Operand,
    },

    /// `in any of the values` condition.
    Any {
        operand: Operand,
        values: Vec<Operand>,
    },

    /// condition expressed by the [`ConditionalFunction`].
    Function(ConditionalFunction),

    /// logical `And` expression.
    And {
        left: Box<ConditionExpression>,
        right: Box<ConditionExpression>,
    },

    /// logical `Or` expression.
    Or {
        left: Box<ConditionExpression>,
        right: Box<ConditionExpression>,
    },

    /// denaial expression.
    Not(Box<ConditionExpression>),

    /// expression wrapped by the parentheses.
    Parentheses(Box<ConditionExpression>),
}

impl fmt::Display for ConditionExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compare {
                left,
                right,
                comperator,
            } => write!(f, "{left} {comperator} {right}"),
            Self::Between { operand, from, to } => write!(f, "{operand} BETWEEN {from} AND {to}"),
            Self::Any { operand, values } => {
                let list = values
                    .iter()
                    .map(|v| format!("{v}"))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(f, "{operand} IN ({list})")
            }
            Self::Function(function) => write!(f, "{function}"),
            Self::And { left, right } => write!(f, "{left} AND {right}"),
            Self::Or { left, right } => write!(f, "{left} OR {right}"),
            Self::Not(expr) => write!(f, "NOT {expr}"),
            Self::Parentheses(expr) => write!(f, "({expr})"),
        }
    }
}

impl From<ConditionExpression> for String {
    fn from(condition: ConditionExpression) -> Self {
        format!("{condition}")
    }
}

impl ConditionExpression {
    pub fn and(self, expr: ConditionExpression) -> Self {
        Self::And {
            left: Box::new(self),
            right: Box::new(expr),
        }
    }

    pub fn or(self, expr: ConditionExpression) -> Self {
        Self::Or {
            left: Box::new(self),
            right: Box::new(expr),
        }
    }
}

/// Create a denial expression.
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::expression::condition::{attribute_exists, not};
/// let expr = not(attribute_exists(op!("#a")));
/// assert_eq!(expr.to_string(), "NOT attribute_exists (#a)");
/// ```
pub fn not(condition_expression: ConditionExpression) -> ConditionExpression {
    ConditionExpression::Not(Box::new(condition_expression))
}

/// Wrap the expression with parentheses.
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::expression::condition::{paren, Condition};
/// let expr_0 = op!("#x").equal(op!(":x"));
/// let expr_1 = op!("#y").equal(op!(":y"));
/// let expr_2 = op!("#z").equal(op!(":z"));
///
/// let expr = paren(expr_0.or(expr_1)).and(expr_2);
/// assert_eq!(expr.to_string(), "(#x = :x OR #y = :y) AND #z = :z");
/// ```
pub fn paren(condition_expression: ConditionExpression) -> ConditionExpression {
    ConditionExpression::Parentheses(Box::new(condition_expression))
}

/// Built in function `attribute_exists`
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::expression::condition::attribute_exists;
/// let expr = attribute_exists(op!("#Pictures", "#SideView"));
/// assert_eq!(expr.to_string(), "attribute_exists (#Pictures.#SideView)");
/// ```
pub fn attribute_exists(operand: impl Into<Operand>) -> ConditionExpression {
    ConditionExpression::Function(ConditionalFunction::AttributeExists(operand.into()))
}

/// Built in function `attribute_not_exists`
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::expression::condition::attribute_not_exists;
/// let expr = attribute_not_exists(op!("Manufacturer"));
/// assert_eq!(expr.to_string(), "attribute_not_exists (Manufacturer)");
/// ```
pub fn attribute_not_exists(operand: impl Into<Operand>) -> ConditionExpression {
    ConditionExpression::Function(ConditionalFunction::AttributeNotExists(operand.into()))
}

/// Built in function `attribute_type`
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::expression::condition::attribute_type;
/// let expr = attribute_type(op!("ProductReviews", "FiveStar"), op!(":v_sub"));
/// assert_eq!(expr.to_string(), "attribute_type (ProductReviews.FiveStar, :v_sub)");
/// ```
pub fn attribute_type(path: impl Into<Operand>, r#type: impl Into<Operand>) -> ConditionExpression {
    ConditionExpression::Function(ConditionalFunction::AttributeType {
        path: path.into(),
        r#type: r#type.into(),
    })
}

/// Built in function `begins_with`
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::expression::condition::begins_with;
/// let expr = begins_with(op!("Pictures", "FrontView"), op!(":v_sub"));
/// assert_eq!(expr.to_string(), "begins_with (Pictures.FrontView, :v_sub)");
/// ```
pub fn begins_with(path: impl Into<Operand>, substr: impl Into<Operand>) -> ConditionExpression {
    ConditionExpression::Function(ConditionalFunction::BeginsWith {
        path: path.into(),
        substr: substr.into(),
    })
}

/// Built in function `contains`
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::expression::condition::contains;
/// let expr = contains(op!("Brand"), op!(":v_sub"));
/// assert_eq!(expr.to_string(), "contains (Brand, :v_sub)");
/// ```
pub fn contains(path: impl Into<Operand>, operand: impl Into<Operand>) -> ConditionExpression {
    ConditionExpression::Function(ConditionalFunction::Contains {
        path: path.into(),
        operand: operand.into(),
    })
}

/// Built in function `size`
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::expression::condition::{Condition, size};
/// let expr = size(op!("Brand")).lte(op!(":v_sub"));
/// assert_eq!(expr.to_string(), "size (Brand) <= :v_sub");
/// ```
pub fn size(operand: Operand) -> Operand {
    Operand::new(format!("size ({operand})"))
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConditionalFunction {
    AttributeExists(Operand),
    AttributeNotExists(Operand),
    AttributeType { path: Operand, r#type: Operand },
    BeginsWith { path: Operand, substr: Operand },
    Contains { path: Operand, operand: Operand },
}

impl fmt::Display for ConditionalFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AttributeExists(operand) => write!(f, "attribute_exists ({operand})"),
            Self::AttributeNotExists(operand) => write!(f, "attribute_not_exists ({operand})"),
            Self::AttributeType { path, r#type } => write!(f, "attribute_type ({path}, {type})"),
            Self::BeginsWith { path, substr } => write!(f, "begins_with ({path}, {substr})"),
            Self::Contains { path, operand } => write!(f, "contains ({path}, {operand})"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Comperator {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

impl fmt::Display for Comperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::Ne => write!(f, "<>"),
            Self::Lt => write!(f, "<"),
            Self::Lte => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::Gte => write!(f, ">="),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::op;

    #[test]
    fn it_creates_equal_to_condition() {
        let expr = op!("#x").equal(op!(":x"));
        assert_eq!(expr.to_string(), "#x = :x");
    }

    #[test]
    fn it_creates_not_equal_to_condition() {
        let expr = op!("#x").ne(op!(":x"));
        assert_eq!(expr.to_string(), "#x <> :x");
    }

    #[test]
    fn it_creates_less_than_condition() {
        let expr = op!("#x").lt(op!(":x"));
        assert_eq!(expr.to_string(), "#x < :x");
    }

    #[test]
    fn it_creates_less_than_or_equal_to_condition() {
        let expr = op!("#x").lte(op!(":x"));
        assert_eq!(expr.to_string(), "#x <= :x");
    }

    #[test]
    fn it_creates_greater_than_condition() {
        let expr = op!("#x").gt(op!(":x"));
        assert_eq!(expr.to_string(), "#x > :x");
    }

    #[test]
    fn it_creates_greater_than_or_equal_to_condition() {
        let expr = op!("#x").gte(op!(":x"));
        assert_eq!(expr.to_string(), "#x >= :x");
    }

    #[test]
    fn it_creates_between_a_and_b_condition() {
        let expr = op!("#x").between(op!(":a"), op!(":b"));
        assert_eq!(expr.to_string(), "#x BETWEEN :a AND :b");
    }

    #[test]
    fn it_creates_in_any_of_the_values_condition() {
        let expr = op!("#x").any([op!(":a"), op!(":b")]);
        assert_eq!(expr.to_string(), "#x IN (:a, :b)");
    }

    #[test]
    fn it_creates_attribute_exists_condition() {
        let expr = attribute_exists(op!("#x"));
        assert_eq!(expr.to_string(), "attribute_exists (#x)");
    }

    #[test]
    fn it_creates_attribute_not_exists_condition() {
        let expr = attribute_not_exists(op!("#x"));
        assert_eq!(expr.to_string(), "attribute_not_exists (#x)");
    }

    #[test]
    fn it_creates_attribute_type_condition() {
        let expr = attribute_type(op!("#x"), op!(":y"));
        assert_eq!(expr.to_string(), "attribute_type (#x, :y)");
    }

    #[test]
    fn it_creates_begins_with_condition() {
        let expr = begins_with(op!("#x"), op!(":y"));
        assert_eq!(expr.to_string(), "begins_with (#x, :y)");
    }

    #[test]
    fn it_creates_contains_condition() {
        let expr = contains(op!("#x"), op!(":y"));
        assert_eq!(expr.to_string(), "contains (#x, :y)");
    }

    #[test]
    fn it_concates_expressions_with_and_operator() {
        let expr_0 = op!("#x").equal(op!(":x"));
        let expr_1 = attribute_exists(op!("#y"));
        assert_eq!(
            expr_0.and(expr_1).to_string(),
            "#x = :x AND attribute_exists (#y)"
        );
    }

    #[test]
    fn it_concates_expressions_with_or_operator() {
        let expr_0 = op!("#x").equal(op!(":x"));
        let expr_1 = attribute_exists(op!("#y"));
        assert_eq!(
            expr_0.or(expr_1).to_string(),
            "#x = :x OR attribute_exists (#y)"
        );
    }

    #[test]
    fn it_creates_denial_expression() {
        let expr = op!("#x").equal(op!(":x"));
        assert_eq!(not(expr).to_string(), "NOT #x = :x");
    }

    #[test]
    fn it_wraps_any_expression_with_parentheses() {
        let expr = op!("#x").equal(op!(":x"));
        assert_eq!(paren(expr).to_string(), "(#x = :x)");
    }
}
