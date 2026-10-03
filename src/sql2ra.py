"""Canonical translation of simple SQL queries into relational algebra.

Supports ``SELECT DISTINCT <cols | *> FROM <tables> [WHERE <conjunction of equalities>]``
and produces a radb AST built from projection, selection, renaming and cross product.
"""
import radb.ast
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison
from sqlparse.tokens import Keyword, DML, Wildcard, Number, String


def extract_sql_clauses(stmt):
    columns = []
    tables = []
    where_cond = []

    select_seen = False
    from_seen = False

    for token in stmt.tokens:
        if token.is_whitespace:
            continue

        # ---- SELECT columns ----
        if token.ttype is DML and token.value.upper() == 'SELECT':
            select_seen = True
            continue

        if select_seen:
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                select_seen = False
                from_seen = True
                continue

            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    columns.append(str(identifier))
            elif isinstance(token, Identifier):
                columns.append(str(token))
            elif token.ttype is Wildcard and str(token).strip() == '*':
                columns.append('*')

        # ---- FROM tables ----
        elif from_seen:
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    tables.append((identifier.get_real_name(), identifier.get_alias()))
            elif isinstance(token, Identifier):
                tables.append((token.get_real_name(), token.get_alias()))

        # ---- WHERE clause ----
        if isinstance(token, Where):
            for cond in token.tokens:
                if isinstance(cond, Comparison):
                    where_cond.append(cond)

    return columns, tables, where_cond


def combine_conditions(where_cond):
    if not where_cond:
        return None
    expr = comparison_to_expr(where_cond[0])
    for comp in where_cond[1:]:
        expr = radb.ast.ValExprBinaryOp(expr, radb.ast.sym.AND, comparison_to_expr(comp))
    return expr


def comparison_to_expr(comp):
    left = None
    right = None

    for token in comp.tokens:
        if isinstance(token, Identifier) or token.ttype in (Number.Integer, Number.Float, String.Single):
            if left is None:
                left = token
            else:
                right = token

    return radb.ast.ValExprBinaryOp(token_to_expr(left), radb.ast.sym.EQ, token_to_expr(right))


def token_to_expr(token):
    if isinstance(token, Identifier):
        return radb.ast.AttrRef(token.get_parent_name(), token.get_real_name())
    elif token.ttype in (Number.Integer, Number.Float):
        return radb.ast.RANumber(token.value)
    elif token.ttype in (String.Single,):
        return radb.ast.RAString(token.value)


def build_relation_from_tables(table_tuples):
    if not table_tuples:
        return None

    def make_rel(name, alias):
        rel = radb.ast.RelRef(name)
        if alias:
            # Only rename table, no columns
            rel = radb.ast.Rename(alias, None, rel)
        return rel

    rel = make_rel(*table_tuples[0])
    for name, alias in table_tuples[1:]:
        rel = radb.ast.Cross(rel, make_rel(name, alias))
    return rel


def parse_attr(col):
    if '.' in col:
        rel, name = col.split('.')
        return radb.ast.AttrRef(rel, name)
    return radb.ast.AttrRef(None, col)


def translate(stmt):
    columns, tables, where_cond = extract_sql_clauses(stmt)

    base_rel = build_relation_from_tables(tables)

    if where_cond:
        cond_expr = combine_conditions(where_cond)
        base_rel = radb.ast.Select(cond_expr, base_rel)

    if columns == ['*']:
        return base_rel

    attrs = [parse_attr(col) for col in columns]
    return radb.ast.Project(attrs, base_rel)
