"""End-to-end tests: SQL -> RA -> optimized RA -> MapReduce, evaluated on the pizza database."""
import json
import unittest

import luigi

import minihive
import ra2mr
from helpers import PIZZA_SCHEMA, prepare_mock_file_system


class End2EndUnoptimizedTests(unittest.TestCase):
    OPTIMIZE = False

    def setUp(self):
        prepare_mock_file_system()

    def _evaluate(self, sqlstring):
        raquery = minihive.optimize_query(sqlstring, PIZZA_SCHEMA, self.OPTIMIZE)

        task = ra2mr.task_factory(raquery, env=ra2mr.ExecEnv.MOCK, optimize=self.OPTIMIZE)
        luigi.build([task], local_scheduler=True)

        if not task.output().exists():
            raise RuntimeError("Task did not produce output!")

        with task.output().open('r') as f:
            return [line.strip() for line in f]

    # --------------------------
    # Tests
    # --------------------------
    def test_select_person(self):
        sqlstring = "select distinct * from Person"
        computed = self._evaluate(sqlstring)
        self.assertEqual(len(computed), 9)

    def test_select_person_age_16(self):
        sqlstring = "select distinct * from Person where age = 16"
        computed = self._evaluate(sqlstring)
        self.assertEqual(len(computed), 1)

    def test_select_person_age_gender(self):
        sqlstring = "select distinct * from Person where gender='female' and age=16"
        computed = self._evaluate(sqlstring)
        self.assertEqual(len(computed), 1)

    def test_project_name(self):
        sqlstring = "select distinct name from Person"
        computed = self._evaluate(sqlstring)
        self.assertEqual(len(computed), 9)

    def test_project_name_age(self):
        sqlstring = "select distinct name, age from Person"
        computed = self._evaluate(sqlstring)
        self.assertEqual(len(computed), 9)
        self.assertIn(
            {"Person.name": "Amy", "Person.age": 16},
            [json.loads(tuple.split(r'	')[1]) for tuple in computed]
        )

    def test_person_join_eats(self):
        sqlstring = "select distinct * from Person, Eats where Person.name = Eats.name"
        computed = self._evaluate(sqlstring)
        self.assertEqual(len(computed), 20)

    def test_project_person_join_eats_join_serves(self):
        sqlstring = (
            "select distinct Person.name, Serves.pizza from Person, Eats, Serves "
            "where Person.name = Eats.name and Eats.pizza = Serves.pizza"
        )
        computed = self._evaluate(sqlstring)
        self.assertEqual(len(computed), 20)

    def test_project_name_of_person(self):
        sqlstring = "select distinct X.name from Person X"
        computed = self._evaluate(sqlstring)
        self.assertEqual(len(computed), 9)
        self.assertIn(
            {"X.name": "Amy"},
            [json.loads(tuple.split(r'	')[1]) for tuple in computed]
        )

    def test_mushroom_lovers(self):
        sqlstring = (
            "select distinct Person.name, Serves.pizzeria from Person, Eats, Serves "
            "where Person.name = Eats.name and Eats.pizza = Serves.pizza "
            "and Eats.pizza = 'mushroom'"
        )
        computed = self._evaluate(sqlstring)
        self.assertEqual(len(computed), 8)

    def test_person_join_eats_join_serves_where(self):
        sqlstring = (
            "select distinct * from Person, Eats, Serves "
            "where Person.name = Eats.name and Eats.pizza = Serves.pizza "
            "and Person.age = 16 and Serves.pizzeria = 'Little Ceasars'"
        )
        computed = self._evaluate(sqlstring)
        self.assertEqual(len(computed), 2)

    def test_eats_join_eats(self):
        sqlstring = "select distinct A.name, B.name from Eats A, Eats B where A.pizza = B.pizza"
        computed = self._evaluate(sqlstring)
        self.assertEqual(len(computed), 63)

    def test_expensive_mushroom_lovers(self):
        sqlstring = (
            "select distinct Serves.pizzeria from Person, Eats, Serves "
            "where Person.name = Eats.name and Eats.pizza = Serves.pizza "
            "and Eats.pizza = 'mushroom' and Serves.price = 11"
        )
        computed = self._evaluate(sqlstring)
        self.assertEqual(len(computed), 1)

    def test_mushroom_lovers_renaming(self):
        sqlstring = (
            "select distinct P.name, S.pizzeria from Person P, Eats E, Serves S "
            "where P.name = E.name and E.pizza = S.pizza "
            "and E.pizza = 'mushroom'"
        )
        computed = self._evaluate(sqlstring)
        self.assertEqual(len(computed), 8)

    def test_4way_join(self):
        sqlstring = (
            "select distinct P1.name "
            "from Person P1, Eats Eats1, Person P2, Eats Eats2 "
            "where P1.name = Eats1.name and P2.name = Eats2.name "
            "and P1.name = P2.name and P1.age = 16"
        )
        computed = self._evaluate(sqlstring)
        self.assertEqual(len(computed), 1)


class End2EndOptimizedTests(End2EndUnoptimizedTests):
    """Same queries and expectations, run through projection push-down and the fused MapReduce tasks."""
    OPTIMIZE = True
