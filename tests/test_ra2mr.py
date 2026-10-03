"""Tests for ra2mr: unoptimized relational algebra -> MapReduce evaluation on the mock file system."""
import json

import luigi
import radb

import ra2mr
from helpers import prepare_mock_file_system


class TestMREvaluation(object):
    person_amy = '{"Person.name": "Amy", "Person.age": 16, "Person.gender": "female"}'
    person_fay = '{"Person.name": "Fay", "Person.age": 21, "Person.gender": "female"}'
    person_hil = '{"Person.name": "Hil", "Person.age": 30, "Person.gender": "female"}'
    person_ben = '{"Person.name": "Ben", "Person.age": 21, "Person.gender": "male"}'

    def setup_method(self, method):
        prepare_mock_file_system()

    def _evaluate(self, querystring):
        raquery = radb.parse.one_statement_from_string(querystring)

        task = ra2mr.task_factory(raquery, env=ra2mr.ExecEnv.MOCK)
        luigi.build([task], local_scheduler=True)

        f = task.output().open('r')
        lines = []
        for line in f:
            lines.append(line)

        f.close()
        return lines

    def _check(self, querystring, expected):
        computed = self._evaluate(querystring)
        assert len(computed) is len(expected)

        for e in expected:
            item_found = False
            e_json_tuple = json.loads(e)

            for c in computed:
                c_relation, c_tuple = c.split(r'	')
                c_json_tuple = json.loads(c_tuple)

                if c_json_tuple == e_json_tuple:
                    item_found = True
                    break
            assert (item_found)

    def test_select_person_gender_female_person(self):
        querystring = r"\select_{Person.gender='female'}(Person);"
        expected = [self.person_amy, self.person_fay, self.person_hil]
        self._check(querystring, expected)

    def test_select_gender_female_person(self):
        querystring = r"\select_{gender='female'}(Person);"
        expected = [self.person_amy, self.person_fay, self.person_hil]
        self._check(querystring, expected)

    def test_select_female_gender_person(self):
        querystring = r"\select_{'female'=gender}(Person);"
        expected = [self.person_amy, self.person_fay, self.person_hil]
        self._check(querystring, expected)

    def test_rename_person(self):
        querystring = r"\rename_{P:*} (Person);"
        computed = self._evaluate(querystring)

        assert len(computed) == 9

        for r in computed:
            relation, tuple = r.split(r'	')
            json_tuple = json.loads(tuple)

            for k in json_tuple.keys():
                # print(k)
                assert k.startswith('P.')

    def test_select_p_gender_female_p(self):
        querystring = r"\select_{P.gender='female'} \rename_{P:*} (Person);"
        computed = self._evaluate(querystring)
        assert len(computed) == 3

    def test_select_age_21_Person(self):
        querystring = r"\select_{age=21}(Person);"
        result = [self.person_fay, self.person_ben]
        self._check(querystring, result)

    def test_select_price_9_Serves(self):
        querystring = r"\select_{price=9}(Serves);"
        computed = self._evaluate(querystring)
        assert len(computed) == 1

    def test_select_person_female_age_16(self):
        querystring = r"\select_{gender='female' and age=16}(Person);"
        result = [self.person_amy]
        self._check(querystring, result)

    def test_select_person_age_3(self):
        querystring = r"\select_{age=3}(Person);"
        self._check(querystring, [])
####################################################
    def test_select_pizza_mushroom(self):
        querystring = r"\project_{pizza} \select_{pizza='mushroom'} Eats;"
        computed = self._evaluate(querystring)
        assert len(computed) == 1

    def test_person_join_eats_mushroom(self):
        querystring = r"Person \join_{Person.name = Eats.name} (\select_{pizza='mushroom'} Eats);"
        computed = self._evaluate(querystring)
        assert len(computed) == 4

        relation, tuple = computed[0].split(r'	')
        json_tuple = json.loads(tuple)
        assert len(json_tuple.keys()) == 5

    def test_project_Person_gender(self):
        querystring = r"\project_{gender} Person;"
        result = ['{"Person.gender": "female"}', '{"Person.gender": "male"}']
        self._check(querystring, result)

    def test_project_person_join_eats(self):
        querystring = r"\project_{Person.name, Eats.pizza} (Person \join_{Person.name = Eats.name} Eats);"
        computed = self._evaluate(querystring)
        assert len(computed) == 20

    def test_female_person_join_eats(self):
        querystring = r"(\select_{gender='female'} Person) \join_{Person.name = Eats.name} Eats;"
        computed = self._evaluate(querystring)
        assert len(computed) == 5

        for line in computed:
            relation, tuple = line.split(r'	')
            json_tuple = json.loads(tuple)
            assert json_tuple["Person.name"] in ["Amy", "Fay", "Hil"]
            assert json_tuple["Person.name"] == json_tuple["Eats.name"]
            assert json_tuple["Person.age"] in [16, 21, 30]
            assert json_tuple["Person.gender"] == "female"

    def test_empty_join(self):
        querystring = r"Person \join_{Person.name = Serves.pizzeria} Serves;"
        computed = self._evaluate(querystring)
        assert computed == []

    def test_person_join_eats_then_join_frequents(self):
        querystring = r"(Person \join_{Person.name = Eats.name} Eats) \join_{Eats.name = Frequents.name} Frequents;"
        computed = self._evaluate(querystring)
        assert len(computed) == 42

    def test_eats_join_person_then_join_frequents(self):
        querystring = r"(Eats \join_{Person.name = Eats.name} Person) \join_{Eats.name = Frequents.name} Frequents;"
        computed = self._evaluate(querystring)
        assert len(computed) == 42

    def test_person_then_join_eats_join_frequents(self):
        querystring = r"Person \join_{Person.name = Eats.name} (Eats \join_{Eats.name = Frequents.name} Frequents);"
        computed = self._evaluate(querystring)
        assert len(computed) == 42

    def test_person_join_eats_join_serves(self):
        querystring = r"Person \join_{Person.name = Eats.name} Eats " \
                      r"\join_{Eats.pizza = Serves.pizza} \select_{price=8}Serves;"
        computed = self._evaluate(querystring)
        assert len(computed) == 8

    def test_person_join_eats_join_serves_dominos(self):
        querystring = r"(Person \join_{Person.name = Eats.name} Eats) " \
                      r"\join_{Eats.pizza = Serves.pizza} (\select_{pizzeria='Dominos'} Serves);"
        computed = self._evaluate(querystring)
        assert len(computed) == 9

    def test_person_join_rename(self):
        querystring = r"(\rename_{A:*} Eats) \join_{A.pizza = B.pizza} (\rename_{B:*} Eats);"
        computed = self._evaluate(querystring)
        assert len(computed) == 94

    def test_person_join_conjunction(self):
        querystring = r"(\rename_{P:*} Person) \join_{P.gender = Q.gender and P.age = Q.age} (\rename_{Q:*} Person);"
        computed = self._evaluate(querystring)
        assert len(computed) == 9