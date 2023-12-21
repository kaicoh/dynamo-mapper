use std::fmt;

use super::Operand;

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
/// # use dynamo_mapper::helpers::expression::{attribute_exists, not};
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
/// # use dynamo_mapper::helpers::expression::{condition, paren};
/// let expr_0 = condition(op!("#x")).eq(op!(":x"));
/// let expr_1 = condition(op!("#y")).eq(op!(":y"));
/// let expr_2 = condition(op!("#z")).eq(op!(":z"));
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
/// # use dynamo_mapper::helpers::expression::attribute_exists;
/// let expr = attribute_exists(op!("#Pictures", "#SideView"));
/// assert_eq!(expr.to_string(), "attribute_exists (#Pictures.#SideView)");
/// ```
pub fn attribute_exists(operand: Operand) -> ConditionExpression {
    ConditionExpression::Function(ConditionalFunction::AttributeExists(operand))
}

/// Built in function `attribute_not_exists`
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::expression::attribute_not_exists;
/// let expr = attribute_not_exists(op!("Manufacturer"));
/// assert_eq!(expr.to_string(), "attribute_not_exists (Manufacturer)");
/// ```
pub fn attribute_not_exists(operand: Operand) -> ConditionExpression {
    ConditionExpression::Function(ConditionalFunction::AttributeNotExists(operand))
}

/// Built in function `attribute_type`
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::expression::attribute_type;
/// let expr = attribute_type(op!("ProductReviews", "FiveStar"), op!(":v_sub"));
/// assert_eq!(expr.to_string(), "attribute_type (ProductReviews.FiveStar, :v_sub)");
/// ```
pub fn attribute_type(path: Operand, r#type: Operand) -> ConditionExpression {
    ConditionExpression::Function(ConditionalFunction::AttributeType { path, r#type })
}

/// Built in function `begins_with`
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::expression::begins_with;
/// let expr = begins_with(op!("Pictures", "FrontView"), op!(":v_sub"));
/// assert_eq!(expr.to_string(), "begins_with (Pictures.FrontView, :v_sub)");
/// ```
pub fn begins_with(path: Operand, substr: Operand) -> ConditionExpression {
    ConditionExpression::Function(ConditionalFunction::BeginsWith { path, substr })
}

/// Built in function `contains`
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::expression::contains;
/// let expr = contains(op!("Brand"), op!(":v_sub"));
/// assert_eq!(expr.to_string(), "contains (Brand, :v_sub)");
/// ```
pub fn contains(path: Operand, operand: Operand) -> ConditionExpression {
    ConditionExpression::Function(ConditionalFunction::Contains { path, operand })
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

#[derive(Debug, Clone, PartialEq)]
pub struct Condition(Operand);

impl Condition {
    /// Create a condition with given operand.
    pub fn new(operand: Operand) -> Self {
        Self(operand)
    }

    /// Create an `equal to` condition expression.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// # use dynamo_mapper::helpers::expression::condition;
    /// let expr = condition(op!("#x")).eq(op!(":x"));
    /// assert_eq!(expr.to_string(), "#x = :x");
    /// ```
    pub fn eq(self, operand: Operand) -> ConditionExpression {
        ConditionExpression::Compare {
            left: self.0,
            right: operand,
            comperator: Comperator::Eq,
        }
    }

    /// Create a `not equal to` condition expression.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// # use dynamo_mapper::helpers::expression::condition;
    /// let expr = condition(op!("#x")).ne(op!(":x"));
    /// assert_eq!(expr.to_string(), "#x <> :x");
    /// ```
    pub fn ne(self, operand: Operand) -> ConditionExpression {
        ConditionExpression::Compare {
            left: self.0,
            right: operand,
            comperator: Comperator::Ne,
        }
    }

    /// Create a `less than` condition expression.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// # use dynamo_mapper::helpers::expression::condition;
    /// let expr = condition(op!("#x")).lt(op!(":x"));
    /// assert_eq!(expr.to_string(), "#x < :x");
    /// ```
    pub fn lt(self, operand: Operand) -> ConditionExpression {
        ConditionExpression::Compare {
            left: self.0,
            right: operand,
            comperator: Comperator::Lt,
        }
    }

    /// Create a `less than or equal to` condition expression.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// # use dynamo_mapper::helpers::expression::condition;
    /// let expr = condition(op!("#x")).lte(op!(":x"));
    /// assert_eq!(expr.to_string(), "#x <= :x");
    /// ```
    pub fn lte(self, operand: Operand) -> ConditionExpression {
        ConditionExpression::Compare {
            left: self.0,
            right: operand,
            comperator: Comperator::Lte,
        }
    }

    /// Create a `greater than` condition expression.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// # use dynamo_mapper::helpers::expression::condition;
    /// let expr = condition(op!("#x")).gt(op!(":x"));
    /// assert_eq!(expr.to_string(), "#x > :x");
    /// ```
    pub fn gt(self, operand: Operand) -> ConditionExpression {
        ConditionExpression::Compare {
            left: self.0,
            right: operand,
            comperator: Comperator::Gt,
        }
    }

    /// Create a `greater than or equal to` condition expression.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// # use dynamo_mapper::helpers::expression::condition;
    /// let expr = condition(op!("#x")).gte(op!(":x"));
    /// assert_eq!(expr.to_string(), "#x >= :x");
    /// ```
    pub fn gte(self, operand: Operand) -> ConditionExpression {
        ConditionExpression::Compare {
            left: self.0,
            right: operand,
            comperator: Comperator::Gte,
        }
    }

    /// Create a `between A and B` condition expression.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// # use dynamo_mapper::helpers::expression::condition;
    /// let expr = condition(op!("#x")).between(op!(":x"), op!(":y"));
    /// assert_eq!(expr.to_string(), "#x BETWEEN :x AND :y");
    /// ```
    pub fn between(self, from: Operand, to: Operand) -> ConditionExpression {
        ConditionExpression::Between {
            operand: self.0,
            from,
            to,
        }
    }

    /// Create an `in any of the values` condition expression.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// # use dynamo_mapper::helpers::expression::condition;
    /// let expr = condition(op!("#x")).any([op!(":x"), op!(":y")]);
    /// assert_eq!(expr.to_string(), "#x IN (:x, :y)");
    /// ```
    pub fn any(self, values: impl IntoIterator<Item = Operand>) -> ConditionExpression {
        ConditionExpression::Any {
            operand: self.0,
            values: values.into_iter().collect(),
        }
    }
}

/// An alias of Condition constructor.
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::expression::condition;
/// let expr = condition(op!("#x")).eq(op!(":x"));
/// assert_eq!(expr.to_string(), "#x = :x");
/// ```
pub fn condition(operand: Operand) -> Condition {
    Condition::new(operand)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::op;

    #[test]
    fn it_creates_equal_to_condition() {
        let expr = condition(op!("#x")).eq(op!(":x"));
        assert_eq!(expr.to_string(), "#x = :x");
    }

    #[test]
    fn it_creates_not_equal_to_condition() {
        let expr = condition(op!("#x")).ne(op!(":x"));
        assert_eq!(expr.to_string(), "#x <> :x");
    }

    #[test]
    fn it_creates_less_than_condition() {
        let expr = condition(op!("#x")).lt(op!(":x"));
        assert_eq!(expr.to_string(), "#x < :x");
    }

    #[test]
    fn it_creates_less_than_or_equal_to_condition() {
        let expr = condition(op!("#x")).lte(op!(":x"));
        assert_eq!(expr.to_string(), "#x <= :x");
    }

    #[test]
    fn it_creates_greater_than_condition() {
        let expr = condition(op!("#x")).gt(op!(":x"));
        assert_eq!(expr.to_string(), "#x > :x");
    }

    #[test]
    fn it_creates_greater_than_or_equal_to_condition() {
        let expr = condition(op!("#x")).gte(op!(":x"));
        assert_eq!(expr.to_string(), "#x >= :x");
    }

    #[test]
    fn it_creates_between_a_and_b_condition() {
        let expr = condition(op!("#x")).between(op!(":a"), op!(":b"));
        assert_eq!(expr.to_string(), "#x BETWEEN :a AND :b");
    }

    #[test]
    fn it_creates_in_any_of_the_values_condition() {
        let expr = condition(op!("#x")).any([op!(":a"), op!(":b")]);
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
        let expr_0 = condition(op!("#x")).eq(op!(":x"));
        let expr_1 = attribute_exists(op!("#y"));
        assert_eq!(
            expr_0.and(expr_1).to_string(),
            "#x = :x AND attribute_exists (#y)"
        );
    }

    #[test]
    fn it_concates_expressions_with_or_operator() {
        let expr_0 = condition(op!("#x")).eq(op!(":x"));
        let expr_1 = attribute_exists(op!("#y"));
        assert_eq!(
            expr_0.or(expr_1).to_string(),
            "#x = :x OR attribute_exists (#y)"
        );
    }

    #[test]
    fn it_creates_denial_expression() {
        let expr = condition(op!("#x")).eq(op!(":x"));
        assert_eq!(not(expr).to_string(), "NOT #x = :x");
    }

    #[test]
    fn it_wraps_any_expression_with_parentheses() {
        let expr = condition(op!("#x")).eq(op!(":x"));
        assert_eq!(paren(expr).to_string(), "(#x = :x)");
    }
}
