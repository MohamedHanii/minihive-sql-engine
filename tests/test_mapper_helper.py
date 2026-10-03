"""Unit tests for ra2mr.MapperHelper, the tuple-level logic shared by all MapReduce tasks."""
import radb.parse
import pytest

import ra2mr
from ra2mr import MapperHelper

AMY = {"Person.name": "Amy", "Person.age": 16, "Person.gender": "female"}


def cond(text):
    """Parse a selection condition by wrapping it in a dummy selection."""
    return radb.parse.one_statement_from_string(rf"\select_{{{text}}} Person;").cond


@pytest.mark.parametrize("text, expected", [
    ("Person.age = 16", True),
    ("age = 16", True),
    ("16 = age", True),
    ("gender = 'female'", True),
    ("gender = 'male'", False),
    ("age < 20", True),
    ("age > 20", False),
    ("age <= 16", True),
    ("age >= 17", False),
    ("gender = 'female' and age = 16", True),
    ("gender = 'female' and age = 17", False),
    ("Other.age = 16", False),
])
def test_eval_condition(text, expected):
    assert MapperHelper.eval_condition(cond(text), AMY) is expected


def test_apply_rename_only_touches_matching_prefix():
    tup = {"Person.name": "Amy", "Eats.pizza": "mushroom"}
    assert MapperHelper.apply_rename("Person", "P", tup) == {"P.name": "Amy", "Eats.pizza": "mushroom"}


def test_apply_projection_qualified_and_unqualified():
    attrs = radb.parse.one_statement_from_string(r"\project_{Person.name, age} Person;").attrs
    assert MapperHelper.apply_projection(attrs, AMY) == {"Person.name": "Amy", "Person.age": 16}


def test_apply_projection_of_missing_attribute_is_empty():
    attrs = radb.parse.one_statement_from_string(r"\project_{pizza} Person;").attrs
    assert MapperHelper.apply_projection(attrs, AMY) == {}


def test_extract_equality_attrs_splits_conjunctions():
    pairs = MapperHelper.extract_equality_attrs(cond("P.gender = Q.gender and P.age = Q.age"))
    assert [(str(left), str(right)) for left, right in pairs] == [("P.gender", "Q.gender"), ("P.age", "Q.age")]


def test_build_join_key_is_identical_for_both_join_sides():
    eq = MapperHelper.extract_equality_attrs(cond("Person.name = Eats.name"))
    left = MapperHelper.build_join_key(eq, AMY)
    right = MapperHelper.build_join_key(eq, {"Eats.name": "Amy", "Eats.pizza": "mushroom"})
    assert left == right == "Amy"


def test_build_join_key_composite():
    eq = MapperHelper.extract_equality_attrs(cond("P.gender = Q.gender and P.age = Q.age"))
    assert MapperHelper.build_join_key(eq, {"Q.gender": "female", "Q.age": 16}) == "female|16"


def test_build_join_key_missing_attribute():
    eq = MapperHelper.extract_equality_attrs(cond("Person.name = Eats.pizza"))
    assert MapperHelper.build_join_key(eq, {"Serves.price": 9}) is None


def test_count_steps():
    ra = radb.parse.one_statement_from_string(
        r"\project_{name} ((\select_{age = 16} Person) \join_{Person.name = Eats.name} Eats);")
    # project + join + select + 2 base relations
    assert ra2mr.count_steps(ra) == 5
