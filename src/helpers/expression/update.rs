use std::fmt;

use super::Operand;

impl Update for Operand {}

pub trait Update: Into<Operand> {
    /// Create `SET A = B` statement.
    ///
    /// ```
    /// use dynamo_mapper::op;
    /// use dynamo_mapper::helpers::expression::update::{set, Update};
    ///
    /// let expr = set(op!("#x").value(op!(":x")));
    /// assert_eq!(expr.to_string(), "SET #x = :x");
    /// ```
    fn value(self, value: impl Into<SetActionValue>) -> SetAction {
        SetAction {
            path: self.into(),
            value: value.into(),
        }
    }

    /// Create `A + B` statement.
    ///
    /// ```
    /// use dynamo_mapper::op;
    /// use dynamo_mapper::helpers::expression::update::{set, Update};
    ///
    /// let expr = set(op!("#x").value(op!(":a").add(op!(":b"))));
    /// assert_eq!(expr.to_string(), "SET #x = :a + :b");
    /// ```
    fn add(self, operand: impl Into<SetActionOperand>) -> SetActionValue {
        SetActionValue::Add(SetActionOperand::Path(self.into()), operand.into())
    }

    /// Create `A - B` statement.
    ///
    /// ```
    /// use dynamo_mapper::op;
    /// use dynamo_mapper::helpers::expression::update::{set, Update};
    ///
    /// let expr = set(op!("#x").value(op!(":a").sub(op!(":b"))));
    /// assert_eq!(expr.to_string(), "SET #x = :a - :b");
    /// ```
    fn sub(self, operand: impl Into<SetActionOperand>) -> SetActionValue {
        SetActionValue::Sub(SetActionOperand::Path(self.into()), operand.into())
    }
}

/// Create `SET A = B` statement.
///
/// ```
/// use dynamo_mapper::op;
/// use dynamo_mapper::helpers::expression::update::{list_append, set, Update};
///
/// let expr = set(op!("#x").value(list_append(op!("#x"), op!(":vals"))));
/// assert_eq!(expr.to_string(), "SET #x = list_append (#x, :vals)");
/// ```
pub fn set(statement: SetAction) -> UpdateExpression {
    UpdateExpression {
        set: vec![statement],
        ..Default::default()
    }
}

/// Create `REMOVE A` statement.
///
/// ```
/// use dynamo_mapper::op;
/// use dynamo_mapper::helpers::expression::update::remove;
///
/// let expr = remove(op!("Brand"))
///     .and(remove(op!("InStock")))
///     .and(remove(op!("QuantityOnHand")));
/// assert_eq!(expr.to_string(), "REMOVE Brand, InStock, QuantityOnHand");
/// ```
pub fn remove(path: impl Into<Operand>) -> UpdateExpression {
    UpdateExpression {
        remove: vec![RemoveAction { path: path.into() }],
        ..Default::default()
    }
}

/// Create `ADD path value` statement.
///
/// ```
/// use dynamo_mapper::op;
/// use dynamo_mapper::helpers::expression::update::add;
///
/// let expr = add(op!("QuantityOnHand"), op!(":q"));
/// assert_eq!(expr.to_string(), "ADD QuantityOnHand :q");
/// ```
pub fn add(path: Operand, value: Operand) -> UpdateExpression {
    UpdateExpression {
        add: vec![AddAction { path, value }],
        ..Default::default()
    }
}

/// Create `DELETE path subset` statement.
///
/// ```
/// use dynamo_mapper::op;
/// use dynamo_mapper::helpers::expression::update::delete;
///
/// let expr = delete(op!("Color"), op!(":p"));
/// assert_eq!(expr.to_string(), "DELETE Color :p");
/// ```
pub fn delete(path: Operand, subset: Operand) -> UpdateExpression {
    UpdateExpression {
        delete: vec![DeleteAction { path, subset }],
        ..Default::default()
    }
}

/// UpdateExpression representation
#[derive(Debug, Default, Clone, PartialEq)]
pub struct UpdateExpression {
    set: Vec<SetAction>,
    remove: Vec<RemoveAction>,
    add: Vec<AddAction>,
    delete: Vec<DeleteAction>,
}

impl UpdateExpression {
    /// Create an empty expression.
    pub fn new() -> Self {
        Self::default()
    }

    /// A method to create multiple update expressions
    ///
    /// ```
    /// use dynamo_mapper::op;
    /// use dynamo_mapper::helpers::expression::update::{remove, set, Update};
    ///
    /// let expr = set(op!("Price").value(op!("Price").sub(op!(":p"))))
    ///     .and(remove(op!("InStock")));
    /// assert_eq!(expr.to_string(), "SET Price = Price - :p REMOVE InStock");
    ///
    /// let expr = set(op!("RelatedItems[1]").value(op!(":newValue")))
    ///     .and(set(op!("Price").value(op!(":newPrice"))));
    /// assert_eq!(expr.to_string(), "SET RelatedItems[1] = :newValue, Price = :newPrice");
    /// ```
    pub fn and(self, mut other: Self) -> Self {
        let mut set = self.set;
        let mut remove = self.remove;
        let mut add = self.add;
        let mut delete = self.delete;

        set.append(&mut other.set);
        remove.append(&mut other.remove);
        add.append(&mut other.add);
        delete.append(&mut other.delete);

        Self {
            set,
            remove,
            add,
            delete,
        }
    }
}

impl fmt::Display for UpdateExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut actions: Vec<String> = vec![];

        if !self.set.is_empty() {
            let set_actions = self
                .set
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>()
                .join(", ");
            actions.push(format!("SET {}", set_actions));
        }

        if !self.remove.is_empty() {
            let remove_actions = self
                .remove
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>()
                .join(", ");
            actions.push(format!("REMOVE {}", remove_actions));
        }

        if !self.add.is_empty() {
            let add_actions = self
                .add
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>()
                .join(", ");
            actions.push(format!("ADD {}", add_actions));
        }

        if !self.delete.is_empty() {
            let delete_actions = self
                .delete
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>()
                .join(", ");
            actions.push(format!("DELETE {}", delete_actions));
        }

        write!(f, "{}", actions.join(" "))
    }
}

impl From<UpdateExpression> for String {
    fn from(condition: UpdateExpression) -> Self {
        format!("{condition}")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetAction {
    path: Operand,
    value: SetActionValue,
}

impl fmt::Display for SetAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.path, self.value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetActionValue {
    Operand(SetActionOperand),
    Add(SetActionOperand, SetActionOperand),
    Sub(SetActionOperand, SetActionOperand),
}

impl fmt::Display for SetActionValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operand(operand) => write!(f, "{operand}"),
            Self::Add(left, right) => write!(f, "{left} + {right}"),
            Self::Sub(left, right) => write!(f, "{left} - {right}"),
        }
    }
}

impl<T: Into<SetActionOperand>> From<T> for SetActionValue {
    fn from(value: T) -> Self {
        Self::Operand(value.into())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetActionOperand {
    Path(Operand),
    Function(SetActionFunction),
}

impl fmt::Display for SetActionOperand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path(operand) => write!(f, "{operand}"),
            Self::Function(function) => write!(f, "{function}"),
        }
    }
}

impl From<Operand> for SetActionOperand {
    fn from(operand: Operand) -> Self {
        Self::Path(operand)
    }
}

impl From<SetActionFunction> for SetActionOperand {
    fn from(function: SetActionFunction) -> Self {
        Self::Function(function)
    }
}

/// Built in function `list_append` for SET update expression.
///
/// ```
/// use dynamo_mapper::op;
/// use dynamo_mapper::helpers::expression::update::{list_append, set, Update};
///
/// let expr = set(op!("#ri").value(list_append(op!("#ri"), op!(":vals"))));
/// assert_eq!(expr.to_string(), "SET #ri = list_append (#ri, :vals)");
/// ```
pub fn list_append(list1: Operand, list2: Operand) -> SetActionFunction {
    SetActionFunction::ListAppend(list1, list2)
}

/// Built in function `if_not_exists` for SET update expression.
///
/// ```
/// use dynamo_mapper::op;
/// use dynamo_mapper::helpers::expression::update::{if_not_exists, set, Update};
///
/// let expr = set(op!("Price").value(if_not_exists(op!("Price"), op!(":p"))));
/// assert_eq!(expr.to_string(), "SET Price = if_not_exists (Price, :p)");
/// ```
pub fn if_not_exists(path: Operand, value: Operand) -> SetActionFunction {
    SetActionFunction::IfNotExists(path, value)
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetActionFunction {
    ListAppend(Operand, Operand),
    IfNotExists(Operand, Operand),
}

impl fmt::Display for SetActionFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ListAppend(list1, list2) => write!(f, "list_append ({list1}, {list2})"),
            Self::IfNotExists(path, value) => write!(f, "if_not_exists ({path}, {value})"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RemoveAction {
    path: Operand,
}

impl fmt::Display for RemoveAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AddAction {
    path: Operand,
    value: Operand,
}

impl fmt::Display for AddAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.path, self.value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteAction {
    path: Operand,
    subset: Operand,
}

impl fmt::Display for DeleteAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.path, self.subset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_creates_set_value_expression() {
        let expr = op!("#x").value(op!(":x"));
        assert_eq!(expr.to_string(), "#x = :x");
    }

    #[test]
    fn it_creates_addition_expression() {
        let expr = op!(":a").add(op!(":b"));
        assert_eq!(expr.to_string(), ":a + :b");
    }

    #[test]
    fn it_creates_subtraction_expression() {
        let expr = op!(":a").sub(op!(":b"));
        assert_eq!(expr.to_string(), ":a - :b");
    }

    #[test]
    fn it_creates_set_update_expression() {
        let expr = set(op!("#x").value(op!(":x")));
        assert_eq!(expr.to_string(), "SET #x = :x");
    }

    #[test]
    fn it_creates_remove_update_expression() {
        let expr = remove(op!("#x"));
        assert_eq!(expr.to_string(), "REMOVE #x");
    }

    #[test]
    fn it_creates_add_update_expression() {
        let expr = add(op!("#x"), op!(":x"));
        assert_eq!(expr.to_string(), "ADD #x :x");
    }

    #[test]
    fn it_creates_delete_update_expression() {
        let expr = delete(op!("#x"), op!(":x"));
        assert_eq!(expr.to_string(), "DELETE #x :x");
    }

    #[test]
    fn it_creates_multiple_update_expression() {
        // concats multiple update expression
        let expr = set(op!("#x").value(op!(":x")))
            .and(set(op!("#y").value(op!(":y"))))
            .and(remove(op!("#x")))
            .and(remove(op!("#y")))
            .and(add(op!("#x"), op!(":x")))
            .and(add(op!("#y"), op!(":y")))
            .and(delete(op!("#x"), op!(":x")))
            .and(delete(op!("#y"), op!(":y")));
        assert_eq!(
            expr.to_string(),
            "SET #x = :x, #y = :y REMOVE #x, #y ADD #x :x, #y :y DELETE #x :x, #y :y",
        );

        let without_set = remove(op!("#x"))
            .and(add(op!("#x"), op!(":x")))
            .and(delete(op!("#x"), op!(":x")));
        assert_eq!(without_set.to_string(), "REMOVE #x ADD #x :x DELETE #x :x");

        let without_remove = set(op!("#x").value(op!(":x")))
            .and(add(op!("#x"), op!(":x")))
            .and(delete(op!("#x"), op!(":x")));
        assert_eq!(
            without_remove.to_string(),
            "SET #x = :x ADD #x :x DELETE #x :x"
        );

        let without_add = set(op!("#x").value(op!(":x")))
            .and(remove(op!("#x")))
            .and(delete(op!("#x"), op!(":x")));
        assert_eq!(
            without_add.to_string(),
            "SET #x = :x REMOVE #x DELETE #x :x"
        );

        let without_delete = set(op!("#x").value(op!(":x")))
            .and(remove(op!("#x")))
            .and(add(op!("#x"), op!(":x")));
        assert_eq!(
            without_delete.to_string(),
            "SET #x = :x REMOVE #x ADD #x :x"
        );
    }

    #[test]
    fn it_creates_list_append_expression() {
        let expr = list_append(op!("#list1"), op!("#list2"));
        assert_eq!(expr.to_string(), "list_append (#list1, #list2)");
    }

    #[test]
    fn it_create_if_not_exists_expression() {
        let expr = if_not_exists(op!("#x"), op!(":x"));
        assert_eq!(expr.to_string(), "if_not_exists (#x, :x)");
    }
}
