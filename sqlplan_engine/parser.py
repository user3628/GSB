"""Recursive-descent parser for the deliberately small SELECT grammar."""

from __future__ import annotations

from .ast import (
    BinaryExpr,
    FunctionCall,
    Identifier,
    InListExpr,
    IsNullExpr,
    JoinRef,
    Literal,
    Node,
    OrderItem,
    Position,
    SelectItem,
    SelectStatement,
    Star,
    TableRef,
    UnaryExpr,
)
from .business import ParseError
from .lexer import Token, tokenize


_ALIAS_STOP_WORDS = {
    "FROM",
    "WHERE",
    "GROUP",
    "HAVING",
    "ORDER",
    "LIMIT",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "FULL",
    "CROSS",
    "OUTER",
    "ON",
    "ASC",
    "DESC",
    "NULLS",
}

_TABLE_STOP_WORDS = _ALIAS_STOP_WORDS | {"SELECT", "AS", "BY", "AND", "OR"}

_RESERVED_TABLE_WORDS = {
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP",
    "BY",
    "HAVING",
    "ORDER",
    "LIMIT",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "FULL",
    "CROSS",
    "OUTER",
    "ON",
    "AS",
    "AND",
    "OR",
    "NOT",
    "IS",
    "NULL",
    "IN",
    "ASC",
    "DESC",
    "NULLS",
    "DISTINCT",
}


class Parser:
    def __init__(self, sql: str) -> None:
        self.sql = sql
        self.tokens = tokenize(sql)
        self.index = 0

    def parse(self) -> SelectStatement:
        statement = self._parse_select()
        if self._match_punc(";"):
            pass
        if not self._at("EOF"):
            self._error("unexpected token after the SELECT statement")
        return statement

    def _parse_select(self) -> SelectStatement:
        select_token = self._expect_keyword("SELECT")
        if self._match_keyword("DISTINCT"):
            self._error("SELECT DISTINCT is outside the supported subset", self._previous())
        items: list[SelectItem] = []
        while True:
            item_start = self._peek()
            expr = self._parse_expression()
            alias: str | None = None
            if self._match_keyword("AS"):
                alias = self._expect_identifier("expected an alias after AS").value
            elif self._can_be_implicit_alias(self._peek()):
                alias = self._advance().value
            items.append(SelectItem(pos=self._position(item_start), expr=expr, alias=alias))
            if not self._match_punc(","):
                break
        self._expect_keyword("FROM")
        from_clause = self._parse_from()
        where = None
        if self._match_keyword("WHERE"):
            where = self._parse_expression()
        group_by: tuple[Node, ...] = ()
        if self._match_keyword("GROUP"):
            self._expect_keyword("BY")
            group_by = tuple(self._parse_expression_list("GROUP BY"))
        having = None
        if self._match_keyword("HAVING"):
            having = self._parse_expression()
        order_by: tuple[OrderItem, ...] = ()
        if self._match_keyword("ORDER"):
            self._expect_keyword("BY")
            order_items: list[OrderItem] = []
            while True:
                item_token = self._peek()
                expr = self._parse_expression()
                direction = "ASC"
                if self._match_keyword("ASC"):
                    direction = "ASC"
                elif self._match_keyword("DESC"):
                    direction = "DESC"
                nulls = None
                if self._match_keyword("NULLS"):
                    if self._match_keyword("FIRST"):
                        nulls = "FIRST"
                    elif self._match_keyword("LAST"):
                        nulls = "LAST"
                    else:
                        self._error("expected FIRST or LAST after NULLS")
                order_items.append(
                    OrderItem(
                        pos=self._position(item_token),
                        expr=expr,
                        direction=direction,
                        nulls=nulls,
                    )
                )
                if not self._match_punc(","):
                    break
            order_by = tuple(order_items)
        limit = None
        if self._match_keyword("LIMIT"):
            limit_token = self._expect("NUMBER", "expected an integer after LIMIT")
            if any(char in limit_token.value for char in ".eE"):
                self._error("LIMIT must be an integer", limit_token)
            limit = int(limit_token.value)
            if limit < 0:
                self._error("LIMIT cannot be negative", limit_token)
        return SelectStatement(
            items=tuple(items),
            from_clause=from_clause,
            where=where,
            group_by=group_by,
            having=having,
            order_by=order_by,
            limit=limit,
            pos=self._position(select_token),
        )

    def _parse_from(self) -> Node:
        result = self._parse_table_factor()
        while True:
            if self._match_punc(","):
                right = self._parse_table_factor()
                result = JoinRef(
                    pos=result.pos,
                    left=result,
                    right=right,
                    join_type="CROSS",
                    condition=None,
                )
                continue
            join_type = self._consume_join_type()
            if join_type is None:
                break
            right = self._parse_table_factor()
            condition = None
            if self._match_keyword("ON"):
                condition = self._parse_expression()
            elif join_type != "CROSS":
                self._error("expected ON after JOIN")
            result = JoinRef(
                pos=result.pos,
                left=result,
                right=right,
                join_type=join_type,
                condition=condition,
            )
        return result

    def _consume_join_type(self) -> str | None:
        if self._match_keyword("JOIN"):
            return "INNER"
        if self._match_keyword("INNER"):
            self._expect_keyword("JOIN")
            return "INNER"
        if self._match_keyword("CROSS"):
            self._expect_keyword("JOIN")
            return "CROSS"
        for keyword, join_type in (("LEFT", "LEFT"), ("RIGHT", "RIGHT"), ("FULL", "FULL")):
            if self._match_keyword(keyword):
                self._match_keyword("OUTER")
                self._expect_keyword("JOIN")
                return join_type
        return None

    def _parse_table_factor(self) -> TableRef:
        token = self._expect_table_identifier("expected a table name after FROM")
        name_parts = [token.value]
        while self._match_punc("."):
            part = self._expect_table_identifier("expected a table name after '.'")
            name_parts.append(part.value)
        alias = None
        if self._match_keyword("AS"):
            alias = self._expect_identifier("expected a table alias after AS").value
        elif self._can_be_implicit_table_alias(self._peek()):
            alias = self._advance().value
        return TableRef(pos=self._position(token), name=".".join(name_parts), alias=alias)

    def _parse_expression_list(self, clause: str) -> list[Node]:
        expressions: list[Node] = []
        while True:
            expressions.append(self._parse_expression())
            if not self._match_punc(","):
                break
        if not expressions:
            self._error(f"expected an expression after {clause}")
        return expressions

    def _parse_expression(self) -> Node:
        return self._parse_or()

    def _parse_or(self) -> Node:
        left = self._parse_and()
        while self._match_keyword("OR"):
            op_token = self._previous()
            right = self._parse_and()
            left = BinaryExpr(pos=self._position(op_token), op="OR", left=left, right=right)
        return left

    def _parse_and(self) -> Node:
        left = self._parse_not()
        while self._match_keyword("AND"):
            op_token = self._previous()
            right = self._parse_not()
            left = BinaryExpr(pos=self._position(op_token), op="AND", left=left, right=right)
        return left

    def _parse_not(self) -> Node:
        if self._match_keyword("NOT"):
            token = self._previous()
            return UnaryExpr(pos=self._position(token), op="NOT", operand=self._parse_not())
        return self._parse_comparison()

    def _parse_comparison(self) -> Node:
        left = self._parse_additive()
        token = self._peek()
        if token.kind == "OP" and token.value in {"=", "!=", "<>", "<", "<=", ">", ">="}:
            self._advance()
            right = self._parse_additive()
            return BinaryExpr(pos=self._position(token), op=token.value, left=left, right=right)
        if self._match_keyword("IS"):
            is_token = self._previous()
            negated = self._match_keyword("NOT")
            self._expect_keyword("NULL")
            return IsNullExpr(pos=self._position(is_token), operand=left, negated=negated)
        negated = False
        if self._match_keyword("NOT"):
            not_token = self._previous()
            if not self._check_keyword("IN"):
                self._error("expected IN after NOT", not_token)
            negated = True
        if self._match_keyword("IN"):
            in_token = self._previous()
            self._expect_punc("(")
            items: list[Node] = []
            if not self._check_punc(")"):
                while True:
                    items.append(self._parse_expression())
                    if not self._match_punc(","):
                        break
            self._expect_punc(")")
            if not items:
                self._error("IN list cannot be empty", in_token)
            return InListExpr(
                pos=self._position(in_token), operand=left, items=tuple(items), negated=negated
            )
        return left

    def _parse_additive(self) -> Node:
        left = self._parse_multiplicative()
        while True:
            token = self._peek()
            if token.kind == "OP" and token.value in {"+", "-", "||"}:
                self._advance()
                right = self._parse_multiplicative()
                left = BinaryExpr(pos=self._position(token), op=token.value, left=left, right=right)
            else:
                return left

    def _parse_multiplicative(self) -> Node:
        left = self._parse_unary()
        while True:
            token = self._peek()
            if token.kind == "OP" and token.value in {"*", "/", "%"}:
                self._advance()
                right = self._parse_unary()
                left = BinaryExpr(pos=self._position(token), op=token.value, left=left, right=right)
            else:
                return left

    def _parse_unary(self) -> Node:
        token = self._peek()
        if token.kind == "OP" and token.value in {"+", "-"}:
            self._advance()
            return UnaryExpr(pos=self._position(token), op=token.value, operand=self._parse_unary())
        return self._parse_primary()

    def _parse_primary(self) -> Node:
        token = self._peek()
        if self._match_punc("("):
            expr = self._parse_expression()
            self._expect_punc(")")
            return expr
        if self._match_op("*"):
            return Star(pos=self._position(token))
        if token.kind == "NUMBER":
            self._advance()
            try:
                if any(char in token.value for char in ".eE"):
                    value: int | float = float(token.value)
                else:
                    value = int(token.value)
            except ValueError:
                self._error("invalid numeric literal", token)
            return Literal(pos=self._position(token), value=value, value_type="number")
        if token.kind == "STRING":
            self._advance()
            return Literal(pos=self._position(token), value=token.value, value_type="string")
        if token.kind in {"WORD", "QIDENT"}:
            if token.kind == "WORD":
                if token.upper == "NULL":
                    self._advance()
                    return Literal(pos=self._position(token), value=None, value_type="null")
                if token.upper in {"TRUE", "FALSE"}:
                    self._advance()
                    return Literal(
                        pos=self._position(token),
                        value=token.upper == "TRUE",
                        value_type="boolean",
                    )
            if self._peek_next().kind == "PUNC" and self._peek_next().value == "(":
                return self._parse_function()
            return self._parse_identifier_expression()
        self._error("expected an expression", token)
        raise AssertionError("unreachable")

    def _parse_function(self) -> FunctionCall:
        name_token = self._advance()
        self._expect_punc("(")
        distinct = self._match_keyword("DISTINCT")
        args: list[Node] = []
        star = False
        if self._match_op("*"):
            star = True
        elif not self._check_punc(")"):
            while True:
                args.append(self._parse_expression())
                if not self._match_punc(","):
                    break
        self._expect_punc(")")
        return FunctionCall(
            pos=self._position(name_token),
            name=name_token.value.upper(),
            args=tuple(args),
            star=star,
            distinct=distinct,
        )

    def _parse_identifier_expression(self) -> Identifier:
        first = self._advance()
        qualifier = None
        name = first.value
        if self._match_punc("."):
            second = self._expect_identifier("expected a column name after '.'")
            qualifier = first.value
            name = second.value
            if self._match_punc("."):
                third = self._expect_identifier("expected a column name after '.'")
                qualifier = f"{first.value}.{second.value}"
                name = third.value
        return Identifier(pos=self._position(first), name=name, qualifier=qualifier)

    def _can_be_implicit_alias(self, token: Token) -> bool:
        if token.kind == "QIDENT":
            return True
        return token.kind == "WORD" and token.upper not in _ALIAS_STOP_WORDS

    def _can_be_implicit_table_alias(self, token: Token) -> bool:
        if token.kind == "QIDENT":
            return True
        return token.kind == "WORD" and token.upper not in _TABLE_STOP_WORDS

    def _peek(self) -> Token:
        return self.tokens[self.index]

    def _peek_next(self) -> Token:
        return self.tokens[min(self.index + 1, len(self.tokens) - 1)]

    def _previous(self) -> Token:
        return self.tokens[self.index - 1]

    def _advance(self) -> Token:
        token = self.tokens[self.index]
        if token.kind != "EOF":
            self.index += 1
        return token

    def _at(self, kind: str) -> bool:
        return self._peek().kind == kind

    def _check_keyword(self, keyword: str) -> bool:
        token = self._peek()
        return token.kind == "WORD" and token.upper == keyword

    def _match_keyword(self, keyword: str) -> bool:
        if self._check_keyword(keyword):
            self._advance()
            return True
        return False

    def _expect_keyword(self, keyword: str) -> Token:
        return self._expect_word(keyword, f"expected {keyword}")

    def _expect_word(self, word: str, message: str) -> Token:
        token = self._peek()
        if token.kind == "WORD" and token.upper == word:
            return self._advance()
        self._error(message, token)
        raise AssertionError("unreachable")

    def _check_punc(self, value: str) -> bool:
        token = self._peek()
        return token.kind == "PUNC" and token.value == value

    def _match_punc(self, value: str) -> bool:
        if self._check_punc(value):
            self._advance()
            return True
        return False

    def _expect_punc(self, value: str) -> Token:
        token = self._peek()
        if token.kind == "PUNC" and token.value == value:
            return self._advance()
        self._error(f"expected '{value}'", token)
        raise AssertionError("unreachable")

    def _match_op(self, value: str) -> bool:
        token = self._peek()
        if token.kind == "OP" and token.value == value:
            self._advance()
            return True
        return False

    def _expect(self, kind: str, message: str) -> Token:
        token = self._peek()
        if token.kind == kind:
            return self._advance()
        self._error(message, token)
        raise AssertionError("unreachable")

    def _expect_identifier(self, message: str) -> Token:
        token = self._peek()
        if token.kind in {"WORD", "QIDENT"}:
            return self._advance()
        self._error(message, token)
        raise AssertionError("unreachable")

    def _expect_table_identifier(self, message: str) -> Token:
        token = self._expect_identifier(message)
        if token.kind == "WORD" and token.upper in _RESERVED_TABLE_WORDS:
            self._error("reserved word cannot be used as an unquoted table name", token)
        return token

    def _position(self, token: Token) -> Position:
        return Position(line=token.line, column=token.column, offset=token.start)

    def _error(self, message: str, token: Token | None = None) -> None:
        token = token or self._peek()
        display = "end of input" if token.kind == "EOF" else repr(token.raw)
        raise ParseError(
            f"{message}; found {display}",
            line=token.line,
            column=token.column,
            offset=token.start,
            sql=self.sql,
        )


def parse_sql(sql: str) -> SelectStatement:
    if not isinstance(sql, str) or not sql.strip():
        raise ParseError(
            "SQL statement is empty",
            line=1,
            column=1,
            offset=0,
            sql=sql if isinstance(sql, str) else "",
        )
    return Parser(sql).parse()

