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
    /// # use dynamo_mapper::helpers::condition_expression::Operand;
    /// let op_0 = Operand::new("#foo");
    /// let op_1 = op!("#foo");
    /// assert_eq!(op_0, op_1);
    /// ```
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Create an `equal` expression.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// let expr = op!("#attr").eq(op!(":val"));
    /// assert_eq!(expr.to_string(), "#attr = :val");
    /// ```
    pub fn eq(self, operand: Operand) -> Expression {
        self.compare("=", operand)
    }

    /// Create a `not equal` expression.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// let expr = op!("#attr").ne(op!(":val"));
    /// assert_eq!(expr.to_string(), "#attr <> :val");
    /// ```
    pub fn ne(self, operand: Operand) -> Expression {
        self.compare("<>", operand)
    }

    /// Create a `less than` expression.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// let expr = op!("#attr").lt(op!(":val"));
    /// assert_eq!(expr.to_string(), "#attr < :val");
    /// ```
    pub fn lt(self, operand: Operand) -> Expression {
        self.compare("<", operand)
    }

    /// Create a `less than or equal` expression.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// let expr = op!("#attr").lte(op!(":val"));
    /// assert_eq!(expr.to_string(), "#attr <= :val");
    /// ```
    pub fn lte(self, operand: Operand) -> Expression {
        self.compare("<=", operand)
    }

    /// Create a `greater than` expression.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// let expr = op!("#attr").gt(op!(":val"));
    /// assert_eq!(expr.to_string(), "#attr > :val");
    /// ```
    pub fn gt(self, operand: Operand) -> Expression {
        self.compare(">", operand)
    }

    /// Create a `greater than or equal` expression.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// let expr = op!("#attr").gte(op!(":val"));
    /// assert_eq!(expr.to_string(), "#attr >= :val");
    /// ```
    pub fn gte(self, operand: Operand) -> Expression {
        self.compare(">=", operand)
    }

    /// Create a `between A and B` expression.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// let expr = op!("#attr").between(op!(":x"), op!(":y"));
    /// assert_eq!(expr.to_string(), "#attr BETWEEN :x AND :y");
    /// ```
    pub fn between(self, from: Operand, to: Operand) -> Expression {
        Expression(format!("{self} BETWEEN {from} AND {to}"))
    }

    /// Create an `in any of the values` expression.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// let expr = op!("#attr").any([op!(":a"), op!(":b"), op!(":c")]);
    /// assert_eq!(expr.to_string(), "#attr IN (:a, :b, :c)");
    /// ```
    pub fn any(self, operands: impl IntoIterator<Item = Operand>) -> Expression {
        let list = operands
            .into_iter()
            .map(|v| v.0)
            .collect::<Vec<String>>()
            .join(", ");
        Expression(format!("{self} IN ({list})"))
    }

    fn compare(self, operator: &str, operand: Operand) -> Expression {
        Expression(format!("{self} {operator} {operand}"))
    }
}

/// A condition expression
#[derive(Debug, Clone, PartialEq)]
pub struct Expression(String);

impl From<Expression> for String {
    fn from(expr: Expression) -> String {
        expr.0
    }
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Expression {
    /// Logical `AND` operator.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// let expr_0 = op!("#attr").lt(op!(":x"));
    /// let expr_1 = op!("#attr").gt(op!(":y"));
    ///
    /// let expr = expr_0.and(expr_1);
    /// assert_eq!(expr.to_string(), "#attr < :x AND #attr > :y");
    /// ```
    pub fn and(self, expr: Expression) -> Expression {
        self.connect("AND", expr)
    }

    /// Logical `OR` operator.
    ///
    /// ```
    /// # use dynamo_mapper::op;
    /// let expr_0 = op!("#attr").lt(op!(":x"));
    /// let expr_1 = op!("#attr").gt(op!(":y"));
    ///
    /// let expr = expr_0.or(expr_1);
    /// assert_eq!(expr.to_string(), "#attr < :x OR #attr > :y");
    /// ```
    pub fn or(self, expr: Expression) -> Expression {
        self.connect("OR", expr)
    }

    fn connect(self, operator: &str, expr: Expression) -> Expression {
        Expression(format!("{self} {operator} {expr}"))
    }
}

/// Wrap the expression with parentheses.
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::condition_expression::wrap;
/// let expr_0 = op!("#a").eq(op!("5"));
/// let expr_1 = op!("#b").eq(op!(":val"));
/// let expr_2 = op!("#c").eq(op!("0"));
///
/// let expr = wrap(expr_0.or(expr_1)).and(expr_2);
/// assert_eq!(expr.to_string(), "(#a = 5 OR #b = :val) AND #c = 0");
/// ```
pub fn wrap(expr: Expression) -> Expression {
    Expression(format!("({expr})"))
}

/// Create a denial expression.
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::condition_expression::not;
/// let expr = not(op!("#a").eq(op!("5")));
/// assert_eq!(expr.to_string(), "NOT #a = 5");
/// ```
pub fn not(expr: Expression) -> Expression {
    Expression(format!("NOT {expr}"))
}

/// Built in function `attribute_exists`
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::condition_expression::attribute_exists;
/// let expr = attribute_exists(op!("#Pictures", "#SideView"));
/// assert_eq!(expr.to_string(), "attribute_exists (#Pictures.#SideView)");
/// ```
pub fn attribute_exists(operand: Operand) -> Expression {
    Expression(format!("attribute_exists ({operand})"))
}

/// Built in function `attribute_not_exists`
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::condition_expression::attribute_not_exists;
/// let expr = attribute_not_exists(op!("Manufacturer"));
/// assert_eq!(expr.to_string(), "attribute_not_exists (Manufacturer)");
/// ```
pub fn attribute_not_exists(operand: Operand) -> Expression {
    Expression(format!("attribute_not_exists ({operand})"))
}

/// Built in function `attribute_type`
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::condition_expression::attribute_type;
/// let expr = attribute_type(op!("ProductReviews", "FiveStar"), op!(":v_sub"));
/// assert_eq!(expr.to_string(), "attribute_type (ProductReviews.FiveStar, :v_sub)");
/// ```
pub fn attribute_type(operand: Operand, ty: Operand) -> Expression {
    Expression(format!("attribute_type ({operand}, {ty})"))
}

/// Built in function `begins_with`
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::condition_expression::begins_with;
/// let expr = begins_with(op!("Pictures", "FrontView"), op!(":v_sub"));
/// assert_eq!(expr.to_string(), "begins_with (Pictures.FrontView, :v_sub)");
/// ```
pub fn begins_with(operand: Operand, substr: Operand) -> Expression {
    Expression(format!("begins_with ({operand}, {substr})"))
}

/// Built in function `contains`
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::condition_expression::contains;
/// let expr = contains(op!("Brand"), op!(":v_sub"));
/// assert_eq!(expr.to_string(), "contains (Brand, :v_sub)");
/// ```
pub fn contains(path: Operand, operand: Operand) -> Expression {
    Expression(format!("contains ({path}, {operand})"))
}

/// Built in function `size`
///
/// ```
/// # use dynamo_mapper::op;
/// # use dynamo_mapper::helpers::condition_expression::size;
/// let expr = size(op!("Brand")).lte(op!(":v_sub"));
/// assert_eq!(expr.to_string(), "size (Brand) <= :v_sub");
/// ```
pub fn size(operand: Operand) -> Operand {
    Operand(format!("size ({operand})"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::op;

    #[test]
    fn comparator_eq() {
        let expr = op!("#a").eq(op!(":v"));
        assert_eq!(expr.to_string(), "#a = :v");
    }

    #[test]
    fn comparator_ne() {
        let expr = op!("#a").ne(op!(":v"));
        assert_eq!(expr.to_string(), "#a <> :v");
    }

    #[test]
    fn comparator_lt() {
        let expr = op!("#a").lt(op!(":v"));
        assert_eq!(expr.to_string(), "#a < :v");
    }

    #[test]
    fn comparator_lte() {
        let expr = op!("#a").lte(op!(":v"));
        assert_eq!(expr.to_string(), "#a <= :v");
    }

    #[test]
    fn comparator_gt() {
        let expr = op!("#a").gt(op!(":v"));
        assert_eq!(expr.to_string(), "#a > :v");
    }

    #[test]
    fn comparator_gte() {
        let expr = op!("#a").gte(op!(":v"));
        assert_eq!(expr.to_string(), "#a >= :v");
    }

    #[test]
    fn expr_between() {
        let expr = op!("#a").between(op!(":x"), op!(":y"));
        assert_eq!(expr.to_string(), "#a BETWEEN :x AND :y");
    }

    #[test]
    fn expr_in() {
        let expr = op!("#a").any([op!(":x"), op!(":y")]);
        assert_eq!(expr.to_string(), "#a IN (:x, :y)");
    }

    #[test]
    fn expr_and() {
        let expr0 = op!("#a").lt(op!(":v"));
        let expr1 = op!("#a").gt(op!(":x"));
        assert_eq!(expr0.and(expr1).to_string(), "#a < :v AND #a > :x");
    }

    #[test]
    fn expr_or() {
        let expr0 = op!("#a").lt(op!(":v"));
        let expr1 = op!("#a").gt(op!(":x"));
        assert_eq!(expr0.or(expr1).to_string(), "#a < :v OR #a > :x");
    }

    #[test]
    fn wrap_expr() {
        let expr = op!("#a").lt(op!(":v"));
        assert_eq!(wrap(expr).to_string(), "(#a < :v)");
    }

    #[test]
    fn not_expr() {
        let expr = op!("#a").lt(op!(":v"));
        assert_eq!(not(expr).to_string(), "NOT #a < :v");
    }

    #[test]
    fn function_attribute_exists() {
        assert_eq!(
            attribute_exists(op!("#a")).to_string(),
            "attribute_exists (#a)"
        );
    }

    #[test]
    fn function_attribute_not_exists() {
        assert_eq!(
            attribute_not_exists(op!("#a")).to_string(),
            "attribute_not_exists (#a)"
        );
    }

    #[test]
    fn function_attribute_type() {
        assert_eq!(
            attribute_type(op!("#a"), op!(":v")).to_string(),
            "attribute_type (#a, :v)"
        );
    }

    #[test]
    fn function_begins_with() {
        assert_eq!(
            begins_with(op!("#a"), op!(":v")).to_string(),
            "begins_with (#a, :v)"
        );
    }

    #[test]
    fn function_contains() {
        assert_eq!(
            contains(op!("#a"), op!(":v")).to_string(),
            "contains (#a, :v)"
        );
    }

    #[test]
    fn function_size() {
        assert_eq!(size(op!("#a")).to_string(), "size (#a)");
    }
}
