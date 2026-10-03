"""Tests for raopt.rule_push_down_projections (the milestone 4 logical optimization).

The rule collects attributes in sets, so the order of attributes inside a generated
projection is not deterministic. These tests therefore compare attribute *sets*.
"""
import radb.ast
import radb.parse

import raopt
from helpers import PIZZA_SCHEMA


def optimize(querystring):
    ra = radb.parse.one_statement_from_string(querystring)
    return raopt.rule_push_down_projections(ra, PIZZA_SCHEMA)


def leaf_projections(node):
    """Map each base relation to the set of attributes projected directly on top of it."""
    found = {}
    if isinstance(node, radb.ast.Project) and isinstance(node.inputs[0], radb.ast.RelRef):
        found.setdefault(node.inputs[0].rel, set()).update(str(a) for a in node.attrs)
    for child in getattr(node, "inputs", None) or []:
        for rel, attrs in leaf_projections(child).items():
            found.setdefault(rel, set()).update(attrs)
    return found


def test_top_level_projection_is_kept():
    out = optimize(r"\project_{name} \select_{age = 16} Person;")
    assert isinstance(out, radb.ast.Project)
    assert [str(a) for a in out.attrs] == ["name"]


def test_selection_attributes_survive_projection():
    out = optimize(r"\project_{name} \select_{age = 16} Person;")
    assert leaf_projections(out) == {"Person": {"Person.name", "Person.age"}}


def test_unused_columns_are_dropped_below_join():
    out = optimize(r"\project_{Person.name, Eats.pizza} (Person \join_{Person.name = Eats.name} Eats);")
    leaves = leaf_projections(out)
    # Person.age and Person.gender are never needed, so they must not be read past the leaf.
    assert leaves["Person"] == {"Person.name"}
    assert leaves["Eats"] == {"Eats.name", "Eats.pizza"}


def test_join_without_top_projection_keeps_all_columns():
    out = optimize(r"Person \join_{Person.name = Eats.name} Eats;")
    assert isinstance(out, radb.ast.Join)
    assert leaf_projections(out) == {
        "Person": {"Person.name", "Person.age", "Person.gender"},
        "Eats": {"Eats.name", "Eats.pizza"},
    }


def test_rename_maps_alias_back_to_base_relation():
    out = optimize(r"\project_{P.name} \rename_{P:*} Person;")
    assert leaf_projections(out) == {"Person": {"Person.name"}}
    assert "\\rename_{P: *}" in str(out)


def test_bare_relation_gets_full_projection():
    out = optimize("Person;")
    assert isinstance(out, radb.ast.Project)
    assert {str(a) for a in out.attrs} == {"Person.name", "Person.age", "Person.gender"}
