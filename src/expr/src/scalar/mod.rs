// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::fmt;
use std::mem;

use serde::{Deserialize, Serialize};

use ore::collections::CollectionExt;
use repr::adt::array::InvalidArrayError;
use repr::adt::datetime::DateTimeUnits;
use repr::adt::regex::Regex;
use repr::strconv::ParseError;
use repr::{ColumnType, Datum, RelationType, Row, RowArena, ScalarType};

use self::func::{BinaryFunc, NullaryFunc, UnaryFunc, VariadicFunc};

pub mod func;
pub mod like_pattern;

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum ScalarExpr {
    /// A column of the input row
    Column(usize),
    /// A literal value.
    /// (Stored as a row, because we can't own a Datum)
    Literal(Row, ColumnType),
    /// A function call that takes no arguments.
    CallNullary(NullaryFunc),
    /// A function call that takes one expression as an argument.
    CallUnary {
        func: UnaryFunc,
        expr: Box<ScalarExpr>,
    },
    /// A function call that takes two expressions as arguments.
    CallBinary {
        func: BinaryFunc,
        expr1: Box<ScalarExpr>,
        expr2: Box<ScalarExpr>,
    },
    /// A function call that takes an arbitrary number of arguments.
    CallVariadic {
        func: VariadicFunc,
        exprs: Vec<ScalarExpr>,
    },
    /// Conditionally evaluated expressions.
    ///
    /// It is important that `then` and `els` only be evaluated if
    /// `cond` is true or not, respectively. This is the only way
    /// users can guard execution (other logical operator do not
    /// short-circuit) and we need to preserve that.
    If {
        cond: Box<ScalarExpr>,
        then: Box<ScalarExpr>,
        els: Box<ScalarExpr>,
    },
}

impl ScalarExpr {
    pub fn columns(is: &[usize]) -> Vec<ScalarExpr> {
        is.iter().map(|i| ScalarExpr::Column(*i)).collect()
    }

    pub fn column(column: usize) -> Self {
        ScalarExpr::Column(column)
    }

    pub fn literal(datum: Datum, typ: ColumnType) -> Self {
        ScalarExpr::Literal(Row::pack(&[datum]), typ)
    }

    pub fn literal_null(typ: ColumnType) -> Self {
        ScalarExpr::literal(Datum::Null, typ)
    }

    pub fn call_unary(self, func: UnaryFunc) -> Self {
        ScalarExpr::CallUnary {
            func,
            expr: Box::new(self),
        }
    }

    pub fn call_binary(self, other: Self, func: BinaryFunc) -> Self {
        ScalarExpr::CallBinary {
            func,
            expr1: Box::new(self),
            expr2: Box::new(other),
        }
    }

    pub fn if_then_else(self, t: Self, f: Self) -> Self {
        ScalarExpr::If {
            cond: Box::new(self),
            then: Box::new(t),
            els: Box::new(f),
        }
    }

    pub fn visit1<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a Self),
    {
        match self {
            ScalarExpr::Column(_) => (),
            ScalarExpr::Literal(_, _) => (),
            ScalarExpr::CallNullary(_) => (),
            ScalarExpr::CallUnary { expr, .. } => {
                f(expr);
            }
            ScalarExpr::CallBinary { expr1, expr2, .. } => {
                f(expr1);
                f(expr2);
            }
            ScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr);
                }
            }
            ScalarExpr::If { cond, then, els } => {
                f(cond);
                f(then);
                f(els);
            }
        }
    }

    pub fn visit<'a, F>(&'a self, f: &mut F)
    where
        F: FnMut(&'a Self),
    {
        self.visit1(|e| e.visit(f));
        f(self);
    }

    pub fn visit1_mut<'a, F>(&'a mut self, mut f: F)
    where
        F: FnMut(&'a mut Self),
    {
        match self {
            ScalarExpr::Column(_) => (),
            ScalarExpr::Literal(_, _) => (),
            ScalarExpr::CallNullary(_) => (),
            ScalarExpr::CallUnary { expr, .. } => {
                f(expr);
            }
            ScalarExpr::CallBinary { expr1, expr2, .. } => {
                f(expr1);
                f(expr2);
            }
            ScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr);
                }
            }
            ScalarExpr::If { cond, then, els } => {
                f(cond);
                f(then);
                f(els);
            }
        }
    }

    pub fn visit_mut<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit1_mut(|e| e.visit_mut(f));
        f(self);
    }

    /// Rewrites column indices with their value in `permutation`.
    ///
    /// This method is applicable even when `permutation` is not a
    /// strict permutation, and it only needs to have entries for
    /// each column referenced in `self`.
    pub fn permute(&mut self, permutation: &[usize]) {
        self.visit_mut(&mut |e| {
            if let ScalarExpr::Column(old_i) = e {
                *old_i = permutation[*old_i];
            }
        });
    }

    /// Rewrites column indices with their value in `permutation`.
    ///
    /// This method is applicable even when `permutation` is not a
    /// strict permutation, and it only needs to have entries for
    /// each column referenced in `self`.
    pub fn permute_map(&mut self, permutation: &std::collections::HashMap<usize, usize>) {
        self.visit_mut(&mut |e| {
            if let ScalarExpr::Column(old_i) = e {
                *old_i = permutation[old_i];
            }
        });
    }

    pub fn support(&self) -> HashSet<usize> {
        let mut support = HashSet::new();
        self.visit(&mut |e| {
            if let ScalarExpr::Column(i) = e {
                support.insert(*i);
            }
        });
        support
    }

    pub fn take(&mut self) -> Self {
        mem::replace(
            self,
            ScalarExpr::literal_null(ScalarType::String.nullable(true)),
        )
    }

    pub fn as_literal(&self) -> Option<Datum> {
        if let ScalarExpr::Literal(row, _column_type) = self {
            Some(row.unpack_first())
        } else {
            None
        }
    }

    pub fn as_literal_str(&self) -> Option<&str> {
        match self.as_literal() {
            Some(Datum::String(s)) => Some(s),
            _ => None,
        }
    }

    pub fn is_literal(&self) -> bool {
        matches!(self, ScalarExpr::Literal(_, _))
    }

    pub fn is_literal_true(&self) -> bool {
        Some(Datum::True) == self.as_literal()
    }

    pub fn is_literal_false(&self) -> bool {
        Some(Datum::False) == self.as_literal()
    }

    pub fn is_literal_null(&self) -> bool {
        Some(Datum::Null) == self.as_literal()
    }

    /// Reduces a complex expression where possible.
    ///
    /// ```rust
    /// use expr::{BinaryFunc, ScalarExpr};
    /// use repr::{ColumnType, Datum, RelationType, ScalarType};
    ///
    /// let expr_0 = ScalarExpr::Column(0);
    /// let expr_t = ScalarExpr::literal(Datum::True, ScalarType::Bool.nullable(false));
    /// let expr_f = ScalarExpr::literal(Datum::False, ScalarType::Bool.nullable(false));
    ///
    /// let mut test =
    /// expr_t
    ///     .clone()
    ///     .call_binary(expr_f.clone(), BinaryFunc::And)
    ///     .if_then_else(expr_0, expr_t.clone());
    ///
    /// let input_type = RelationType::new(vec![ScalarType::Int32.nullable(false)]);
    /// assert!(test.reduce(&input_type).is_ok());
    /// assert_eq!(test, expr_t);
    /// ```
    pub fn reduce(&mut self, relation_type: &RelationType) -> Result<(), EvalError> {
        let temp_storage = &RowArena::new();
        self.reduce_inner(temp_storage, relation_type)
    }

    fn reduce_inner(
        &mut self,
        temp_storage: &RowArena,
        relation_type: &RelationType,
    ) -> Result<(), EvalError> {
        let eval = |e: &ScalarExpr| -> Result<ScalarExpr, EvalError> {
            Ok(ScalarExpr::literal(
                e.eval(&[], temp_storage)?,
                e.typ(&relation_type),
            ))
        };
        match self {
            ScalarExpr::Column(_) | ScalarExpr::Literal(_, _) | ScalarExpr::CallNullary(_) => (),
            ScalarExpr::CallUnary { func, expr } => {
                expr.reduce_inner(temp_storage, relation_type)?;
                if expr.is_literal() {
                    *self = eval(self)?;
                } else if *func == UnaryFunc::IsNull {
                    // (<expr1> <op> <expr2>) IS NULL can often be simplified to
                    // (<expr1> IS NULL) OR (<expr2> IS NULL).
                    if let ScalarExpr::CallBinary { func, expr1, expr2 } = &mut **expr {
                        if func.propagates_nulls() && !func.introduces_nulls() {
                            let expr1 = expr1.take().call_unary(UnaryFunc::IsNull);
                            let expr2 = expr2.take().call_unary(UnaryFunc::IsNull);
                            *self = expr1.call_binary(expr2, BinaryFunc::Or);
                        }
                    }
                }
            }
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                expr1.reduce_inner(temp_storage, relation_type)?;
                expr2.reduce_inner(temp_storage, relation_type)?;
                if expr1.is_literal() && expr2.is_literal() {
                    *self = eval(self)?;
                } else if (expr1.is_literal_null() || expr2.is_literal_null())
                    && func.propagates_nulls()
                {
                    *self = ScalarExpr::literal_null(self.typ(relation_type));
                } else if *func == BinaryFunc::IsLikePatternMatch && expr2.is_literal() {
                    // We can at least precompile the regex.
                    let pattern = expr2.as_literal_str().unwrap();
                    *self = match like_pattern::build_regex(&pattern) {
                        Ok(regex) => expr1
                            .take()
                            .call_unary(UnaryFunc::IsRegexpMatch(Regex(regex))),
                        Err(_) => ScalarExpr::literal_null(self.typ(&relation_type)),
                    };
                } else if let BinaryFunc::IsRegexpMatch { case_insensitive } = func {
                    if let ScalarExpr::Literal(row, _) = &**expr2 {
                        let flags = if *case_insensitive { "i" } else { "" };
                        let regex = func::build_regex(row.unpack_first().unwrap_str(), flags)?;
                        *self = expr1
                            .take()
                            .call_unary(UnaryFunc::IsRegexpMatch(Regex(regex)));
                    }
                } else if *func == BinaryFunc::DatePartInterval && expr1.is_literal() {
                    let units = expr1.as_literal_str().unwrap();
                    *self = match units.parse::<DateTimeUnits>() {
                        Ok(units) => ScalarExpr::CallUnary {
                            func: UnaryFunc::DatePartInterval(units),
                            expr: Box::new(expr2.take()),
                        },
                        Err(_) => ScalarExpr::literal_null(self.typ(&relation_type)),
                    }
                } else if *func == BinaryFunc::DatePartTimestamp && expr1.is_literal() {
                    let units = expr1.as_literal_str().unwrap();
                    *self = match units.parse::<DateTimeUnits>() {
                        Ok(units) => ScalarExpr::CallUnary {
                            func: UnaryFunc::DatePartTimestamp(units),
                            expr: Box::new(expr2.take()),
                        },
                        Err(_) => ScalarExpr::literal_null(self.typ(&relation_type)),
                    }
                } else if *func == BinaryFunc::DatePartTimestampTz && expr1.is_literal() {
                    let units = expr1.as_literal_str().unwrap();
                    *self = match units.parse::<DateTimeUnits>() {
                        Ok(units) => ScalarExpr::CallUnary {
                            func: UnaryFunc::DatePartTimestampTz(units),
                            expr: Box::new(expr2.take()),
                        },
                        Err(_) => ScalarExpr::literal_null(self.typ(&relation_type)),
                    }
                } else if *func == BinaryFunc::DateTruncTimestamp && expr1.is_literal() {
                    let units = expr1.as_literal_str().unwrap();
                    *self = match units.parse::<DateTimeUnits>() {
                        Ok(units) => ScalarExpr::CallUnary {
                            func: UnaryFunc::DateTruncTimestamp(units),
                            expr: Box::new(expr2.take()),
                        },
                        Err(_) => ScalarExpr::literal_null(self.typ(&relation_type)),
                    }
                } else if *func == BinaryFunc::DateTruncTimestampTz && expr1.is_literal() {
                    let units = expr1.as_literal_str().unwrap();
                    *self = match units.parse::<DateTimeUnits>() {
                        Ok(units) => ScalarExpr::CallUnary {
                            func: UnaryFunc::DateTruncTimestampTz(units),
                            expr: Box::new(expr2.take()),
                        },
                        Err(_) => ScalarExpr::literal_null(self.typ(&relation_type)),
                    }
                } else if *func == BinaryFunc::And {
                    // If we are here, not both inputs are literals.
                    if expr1.is_literal_false() || expr2.is_literal_true() {
                        *self = expr1.take();
                    } else if expr2.is_literal_false() || expr1.is_literal_true() {
                        *self = expr2.take();
                    } else if expr1 == expr2 {
                        *self = expr1.take();
                    }
                } else if *func == BinaryFunc::Or {
                    // If we are here, not both inputs are literals.
                    if expr1.is_literal_true() || expr2.is_literal_false() {
                        *self = expr1.take();
                    } else if expr2.is_literal_true() || expr1.is_literal_false() {
                        *self = expr2.take();
                    } else if expr1 == expr2 {
                        *self = expr1.take();
                    }
                }
            }
            ScalarExpr::CallVariadic {
                func: VariadicFunc::Coalesce,
                exprs,
            } => {
                let mut last_i = 0;
                for (i, e) in exprs.iter_mut().enumerate() {
                    last_i = i;
                    e.reduce_inner(temp_storage, relation_type)?;
                    if e.is_literal() && !e.is_literal_null() {
                        break;
                    }
                }

                // All arguments after the first literal are ignored, so throw them away. This
                // intentionally throws away errors that can never happen.
                exprs.truncate(last_i + 1);

                // If all inputs are null, output is null. This check must
                // be done before `exprs.retain...` because `self.typ` requires
                // > 0 `exprs` remain.
                if exprs.iter().all(|expr| expr.is_literal_null()) {
                    *self = ScalarExpr::literal_null(self.typ(&relation_type));
                    return Ok(());
                }

                // Remove any null values if not all values are null.
                exprs.retain(|e| !e.is_literal_null());

                // Deduplicate arguments in cases like `coalesce(#0, #0)`.
                exprs.dedup();

                if exprs.len() == 1 {
                    // Only one argument, so the coalesce is a no-op.
                    *self = exprs[0].take();
                }
            }
            ScalarExpr::CallVariadic { func, exprs } => {
                for expr in exprs.iter_mut() {
                    expr.reduce_inner(temp_storage, relation_type)?;
                }
                if exprs.iter().all(|e| e.is_literal()) {
                    *self = eval(self)?;
                } else if func.propagates_nulls() && exprs.iter().any(|e| e.is_literal_null()) {
                    *self = ScalarExpr::literal_null(self.typ(&relation_type));
                } else if *func == VariadicFunc::RegexpMatch {
                    if exprs[1].is_literal() && exprs.get(2).map_or(true, |e| e.is_literal()) {
                        let needle = exprs[1].as_literal_str().unwrap();
                        let flags = match exprs.len() {
                            3 => exprs[2].as_literal_str().unwrap(),
                            _ => "",
                        };
                        let regex = func::build_regex(needle, flags)?;
                        *self = mem::take(exprs)
                            .into_first()
                            .call_unary(UnaryFunc::RegexpMatch(Regex(regex)));
                    }
                }
            }
            ScalarExpr::If { cond, then, els } => {
                cond.reduce_inner(temp_storage, relation_type)?;
                then.reduce_inner(temp_storage, relation_type)?;
                els.reduce_inner(temp_storage, relation_type)?;
                if let Some(literal) = cond.as_literal() {
                    match literal {
                        Datum::True => *self = then.take(),
                        Datum::False | Datum::Null => *self = els.take(),
                        _ => unreachable!(),
                    }
                } else if then == els {
                    *self = then.take();
                } else if then.is_literal() && els.is_literal() {
                    match (then.as_literal(), els.as_literal()) {
                        (Some(Datum::True), _) => {
                            *self = cond.take().call_binary(els.take(), BinaryFunc::Or);
                        }
                        (Some(Datum::False), _) => {
                            *self = cond
                                .take()
                                .call_unary(UnaryFunc::Not)
                                .call_binary(els.take(), BinaryFunc::And);
                        }
                        (_, Some(Datum::True)) => {
                            *self = cond
                                .take()
                                .call_unary(UnaryFunc::Not)
                                .call_binary(then.take(), BinaryFunc::Or);
                        }
                        (_, Some(Datum::False)) => {
                            *self = cond.take().call_binary(then.take(), BinaryFunc::And);
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    /// Adds any columns that *must* be non-Null for `self` to be non-Null.
    pub fn non_null_requirements(&self, columns: &mut HashSet<usize>) {
        match self {
            ScalarExpr::Column(col) => {
                columns.insert(*col);
            }
            ScalarExpr::Literal(..) => {}
            ScalarExpr::CallNullary(_) => (),
            ScalarExpr::CallUnary { func, expr } => {
                if func.propagates_nulls() {
                    expr.non_null_requirements(columns);
                }
            }
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                if func.propagates_nulls() {
                    expr1.non_null_requirements(columns);
                    expr2.non_null_requirements(columns);
                }
            }
            ScalarExpr::CallVariadic { func, exprs } => {
                if func.propagates_nulls() {
                    for expr in exprs {
                        expr.non_null_requirements(columns);
                    }
                }
            }
            ScalarExpr::If { .. } => (),
        }
    }

    pub fn typ(&self, relation_type: &RelationType) -> ColumnType {
        match self {
            ScalarExpr::Column(i) => relation_type.column_types[*i].clone(),
            ScalarExpr::Literal(_, typ) => typ.clone(),
            ScalarExpr::CallNullary(func) => func.output_type(),
            ScalarExpr::CallUnary { expr, func } => func.output_type(expr.typ(relation_type)),
            ScalarExpr::CallBinary { expr1, expr2, func } => {
                func.output_type(expr1.typ(relation_type), expr2.typ(relation_type))
            }
            ScalarExpr::CallVariadic { exprs, func } => {
                func.output_type(exprs.iter().map(|e| e.typ(relation_type)).collect())
            }
            ScalarExpr::If { cond: _, then, els } => {
                let then_type = then.typ(relation_type);
                let else_type = els.typ(relation_type);
                debug_assert!(then_type.scalar_type == else_type.scalar_type);
                ColumnType {
                    nullable: then_type.nullable || else_type.nullable,
                    scalar_type: then_type.scalar_type,
                }
            }
        }
    }

    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        match self {
            ScalarExpr::Column(index) => Ok(datums[*index].clone()),
            ScalarExpr::Literal(row, _column_type) => Ok(row.unpack_first()),
            // Nullary functions must be transformed away before evaluation.
            // Their purpose is as a placeholder for data that is not known at
            // plan time but can be inlined before runtime.
            ScalarExpr::CallNullary(_) => Err(EvalError::Internal(
                "cannot evaluate nullary function".into(),
            )),
            ScalarExpr::CallUnary { func, expr } => func.eval(datums, temp_storage, expr),
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                func.eval(datums, temp_storage, expr1, expr2)
            }
            ScalarExpr::CallVariadic { func, exprs } => func.eval(datums, temp_storage, exprs),
            ScalarExpr::If { cond, then, els } => match cond.eval(datums, temp_storage)? {
                Datum::True => then.eval(datums, temp_storage),
                Datum::False | Datum::Null => els.eval(datums, temp_storage),
                d => Err(EvalError::Internal(format!(
                    "if condition evaluated to non-boolean datum: {:?}",
                    d
                ))),
            },
        }
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum EvalError {
    DivisionByZero,
    NumericFieldOverflow,
    FloatOutOfRange,
    IntegerOutOfRange,
    IntervalOutOfRange,
    TimestampOutOfRange,
    InvalidDimension {
        max_dim: usize,
        val: i64,
    },
    InvalidArray(InvalidArrayError),
    InvalidEncodingName(String),
    InvalidHashAlgorithm(String),
    InvalidByteSequence {
        byte_sequence: String,
        encoding_name: String,
    },
    InvalidRegex(String),
    InvalidRegexFlag(char),
    InvalidParameterValue(String),
    NegSqrt,
    UnknownUnits(String),
    UnsupportedDateTimeUnits(DateTimeUnits),
    UnterminatedLikeEscapeSequence,
    Parse(ParseError),
    Internal(String),
}

impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EvalError::DivisionByZero => f.write_str("division by zero"),
            EvalError::NumericFieldOverflow => f.write_str("numeric field overflow"),
            EvalError::FloatOutOfRange => f.write_str("float out of range"),
            EvalError::IntegerOutOfRange => f.write_str("integer out of range"),
            EvalError::IntervalOutOfRange => f.write_str("interval out of range"),
            EvalError::TimestampOutOfRange => f.write_str("timestamp out of range"),
            EvalError::InvalidDimension { max_dim, val } => write!(
                f,
                "invalid dimension: {}; must use value within [1, {}]",
                val, max_dim
            ),
            EvalError::InvalidArray(e) => e.fmt(f),
            EvalError::InvalidEncodingName(name) => write!(f, "invalid encoding name '{}'", name),
            EvalError::InvalidHashAlgorithm(alg) => write!(f, "invalid hash algorithm '{}'", alg),
            EvalError::InvalidByteSequence {
                byte_sequence,
                encoding_name,
            } => write!(
                f,
                "invalid byte sequence '{}' for encoding '{}'",
                byte_sequence, encoding_name
            ),
            EvalError::NegSqrt => f.write_str("cannot take square root of a negative number"),
            EvalError::InvalidRegex(e) => write!(f, "invalid regular expression: {}", e),
            EvalError::InvalidRegexFlag(c) => write!(f, "invalid regular expression flag: {}", c),
            EvalError::InvalidParameterValue(s) => f.write_str(s),
            EvalError::UnknownUnits(units) => write!(f, "unknown units '{}'", units),
            EvalError::UnsupportedDateTimeUnits(units) => {
                write!(f, "unsupported timestamp units '{}'", units)
            }
            EvalError::UnterminatedLikeEscapeSequence => {
                f.write_str("unterminated escape sequence in LIKE")
            }
            EvalError::Parse(e) => e.fmt(f),
            EvalError::Internal(s) => write!(f, "internal error: {}", s),
        }
    }
}

impl std::error::Error for EvalError {}

impl From<ParseError> for EvalError {
    fn from(e: ParseError) -> EvalError {
        EvalError::Parse(e)
    }
}

impl From<InvalidArrayError> for EvalError {
    fn from(e: InvalidArrayError) -> EvalError {
        EvalError::InvalidArray(e)
    }
}

impl From<regex::Error> for EvalError {
    fn from(e: regex::Error) -> EvalError {
        EvalError::InvalidRegex(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reduce() {
        let relation_type = RelationType::new(vec![
            ScalarType::Int64.nullable(true),
            ScalarType::Int64.nullable(true),
            ScalarType::Int64.nullable(false),
        ]);
        let col = |i| ScalarExpr::Column(i);
        let lit = |i| ScalarExpr::literal(Datum::Int64(i), ScalarType::Int64.nullable(false));
        let null = || ScalarExpr::literal_null(ScalarType::Int64.nullable(true));

        let add_err = ScalarExpr::CallBinary {
            func: BinaryFunc::AddInt64,
            expr1: Box::new(lit(i64::MAX)),
            expr2: Box::new(lit(1)),
        };
        let div_err = ScalarExpr::CallBinary {
            func: BinaryFunc::DivInt64,
            expr1: Box::new(lit(1)),
            expr2: Box::new(lit(0)),
        };

        struct TestCase {
            input: ScalarExpr,
            output: Result<ScalarExpr, EvalError>,
        }

        let test_cases = vec![
            TestCase {
                input: ScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![lit(1)],
                },
                output: Ok(lit(1)),
            },
            TestCase {
                input: ScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![lit(1), lit(2)],
                },
                output: Ok(lit(1)),
            },
            TestCase {
                input: ScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![null(), lit(2), null()],
                },
                output: Ok(lit(2)),
            },
            TestCase {
                input: ScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![null(), col(0), null(), col(1), lit(2), lit(3)],
                },
                output: Ok(ScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![col(0), col(1), lit(2)],
                }),
            },
            TestCase {
                input: ScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![col(0), col(2), col(1)],
                },
                output: Ok(ScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![col(0), col(2), col(1)],
                }),
            },
            TestCase {
                input: ScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![lit(1), div_err.clone()],
                },
                output: Ok(lit(1)),
            },
            TestCase {
                input: ScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![col(0), div_err.clone()],
                },
                output: Err(EvalError::DivisionByZero),
            },
            TestCase {
                input: ScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![null(), div_err.clone(), add_err],
                },
                output: Err(EvalError::DivisionByZero),
            },
            TestCase {
                input: ScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![col(0), div_err],
                },
                output: Err(EvalError::DivisionByZero),
            },
        ];

        for tc in test_cases {
            let mut actual = tc.input.clone();
            let result = actual.reduce(&relation_type).and(Ok(actual));
            assert!(
                result == tc.output,
                "input: {}\nactual: {:?}\nexpected: {:?}",
                tc.input,
                result,
                tc.output
            );
        }
    }
}
