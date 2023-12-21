#[macro_export]
macro_rules! op {
    ($val:expr) => {
        $crate::helpers::expression::Operand::new(format!("{}", $val))
    };
    ($first:expr, $($rest:expr),*) => {{
        let mut val = format!("{}", $first);
        $(
            val = format!("{}.{}", val, $rest);
        )*
        $crate::helpers::expression::Operand::new(val)
    }};
}

#[cfg(test)]
mod tests {
    use crate::helpers::expression::Operand;

    #[test]
    fn op_macro_creates_an_operand() {
        assert_eq!(op!("foo"), Operand::new("foo"));
    }

    #[test]
    fn op_macro_creates_an_operand_with_expression() {
        let f = "foobar";
        assert_eq!(op!(f), Operand::new("foobar"));
    }

    #[test]
    fn op_macro_creates_an_operand_from_multiple_expression() {
        assert_eq!(op!("foo", "bar"), Operand::new("foo.bar"));

        let bar = "bar";
        assert_eq!(op!("foo", bar, "baz"), Operand::new("foo.bar.baz"));
    }
}
