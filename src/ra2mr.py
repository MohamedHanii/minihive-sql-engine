"""Compile relational algebra (radb) query trees into chained Luigi MapReduce jobs."""
from enum import Enum
import json
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from luigi.mock import MockTarget
import radb
import radb.ast
import radb.parse

def rel_id(raquery):
    if isinstance(raquery, radb.ast.RelRef):
        return raquery.rel
    
    if isinstance(raquery, radb.ast.Rename):
         return raquery.relname

    if isinstance(raquery, radb.ast.Join):
         return f"Join({rel_id(raquery.inputs[0])},{rel_id(raquery.inputs[1])})"
    
    if hasattr(raquery, "inputs") and raquery.inputs:
         return rel_id(raquery.inputs[0])
    
    return "data"

'''
Control where the input data comes from, and where output data should go.
'''

class ExecEnv(Enum):
    LOCAL = 1  # read/write local files
    HDFS = 2  # read/write HDFS
    MOCK = 3  # read/write mock data to an in-memory file system.


'''
Switches between different execution environments and file systems.
'''


class OutputMixin(luigi.Task):
    exec_environment = luigi.EnumParameter(enum=ExecEnv, default=ExecEnv.HDFS)

    def get_output(self, fn):
        if self.exec_environment == ExecEnv.HDFS:
            return luigi.contrib.hdfs.HdfsTarget(fn)
        elif self.exec_environment == ExecEnv.MOCK:
            return MockTarget(fn)
        else:
            return luigi.LocalTarget(fn)


class InputData(OutputMixin):
    filename = luigi.Parameter()

    def output(self):
        return self.get_output(self.filename)


'''
Counts the number of steps / luigi tasks that we need for evaluating this query.
'''


def count_steps(raquery):
    assert (isinstance(raquery, radb.ast.Node))

    if (isinstance(raquery, radb.ast.Select) or isinstance(raquery, radb.ast.Project) or
            isinstance(raquery, radb.ast.Rename)):
        return 1 + count_steps(raquery.inputs[0])

    elif isinstance(raquery, radb.ast.Join):
        return 1 + count_steps(raquery.inputs[0]) + count_steps(raquery.inputs[1])

    elif isinstance(raquery, radb.ast.RelRef):
        return 1

    else:
        raise Exception("count_steps: Cannot handle operator " + str(type(raquery)) + ".")


class RelAlgQueryTask(luigi.contrib.hadoop.JobTask, OutputMixin):
    '''
    Each physical operator knows its (partial) query string.
    As a string, the value of this parameter can be searialized
    and shipped to the data node in the Hadoop cluster.
    '''
    querystring = luigi.Parameter()

    '''
    Each physical operator within a query has its own step-id.
    This is used to rename the temporary files for exhanging
    data between chained MapReduce jobs.
    '''
    step = luigi.IntParameter(default=1)
    optimize = luigi.BoolParameter(default=False)
    '''
    In HDFS, we call the folders for temporary data tmp1, tmp2, ...
    In the local or mock file system, we call the files tmp1.tmp...
    '''

    def output(self):
        if self.exec_environment == ExecEnv.HDFS:
            filename = "tmp" + str(self.step)
        else:
            filename = "tmp" + str(self.step) + ".tmp"
        return self.get_output(filename)


'''
Given the radb-string representation of a relational algebra query,
this produces a tree of luigi tasks with the physical query operators.
'''


class PlanNode:
    pass


class SourceNode(PlanNode):
    def __init__(self, rel):
        self.rel = rel


class MapChainNode(PlanNode):
    def __init__(self, ops, child):
        self.ops = ops
        self.child = child


class JoinNode(PlanNode):
    def __init__(self, left, right, condition):
        self.left = left
        self.right = right
        self.condition = condition


def build_execution_plan(node, stop_at_join=False):
    # Base table
    if isinstance(node, radb.ast.RelRef):
        return SourceNode(node)

    # Join
    if isinstance(node, radb.ast.Join):
        if stop_at_join:
            return SourceNode(node)
        left_plan = build_execution_plan(node.inputs[0], stop_at_join=True)
        right_plan = build_execution_plan(node.inputs[1], stop_at_join=True)
        return JoinNode(left_plan, right_plan, node.cond)

    # MAP chain
    if isinstance(node, (radb.ast.Select, radb.ast.Project, radb.ast.Rename)):
        ops = []
        current = node

        while isinstance(current, (radb.ast.Select, radb.ast.Project, radb.ast.Rename)):
            ops.append(current)
            current = current.inputs[0]

        child_plan = build_execution_plan(current, stop_at_join=stop_at_join)
        return MapChainNode(ops[::-1], child_plan)

    raise NotImplementedError(type(node))

def task_factory(raquery, step=1, env=ExecEnv.HDFS, optimize=False):
    assert isinstance(raquery, radb.ast.Node)

    if optimize:
        plan = build_execution_plan(raquery)
        if isinstance(plan, JoinNode):
            return EnhancedJoinTask(querystring=str(raquery) + ";", step=step, exec_environment=env, optimize=optimize)
        if isinstance(plan, MapChainNode):
            return MapperChainTask(querystring=str(raquery) + ";", step=step, exec_environment=env, optimize=optimize)
        if isinstance(plan, SourceNode):
            if isinstance(raquery, radb.ast.RelRef):
                return InputData(filename=raquery.rel + ".json", exec_environment=env)
                
            return task_factory(raquery, step=step, env=env, optimize=False)


    # Non-optimized fallback
    if isinstance(raquery, radb.ast.Select):
        return SelectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)
    elif isinstance(raquery, radb.ast.Rename):
        return RenameTask(querystring=str(raquery) + ";", step=step, exec_environment=env)
    elif isinstance(raquery, radb.ast.Project):
        return ProjectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)
    elif isinstance(raquery, radb.ast.Join):
        return JoinTask(querystring=str(raquery) + ";", step=step, exec_environment=env)
    elif isinstance(raquery, radb.ast.RelRef):
        return InputData(filename=raquery.rel + ".json", exec_environment=env)
    else:
        raise NotImplementedError(type(raquery))


class MapperHelper:
    '''
    Helper class containing reusable mapper logic for all relational algebra operations.
    All methods are static to allow easy reuse across different task classes.
    '''
    @staticmethod
    def eval_condition(cond, json_tuple):
        '''Evaluate a selection condition against a JSON tuple.'''
        if isinstance(cond, radb.ast.ValExprBinaryOp):
            op = cond.op
            left, right = cond.inputs
            
            if op == radb.ast.sym.AND:
                return MapperHelper.eval_condition(left, json_tuple) and \
                       MapperHelper.eval_condition(right, json_tuple)
            
            return MapperHelper.eval_atomic(cond, json_tuple)
        
        return False
    
    @staticmethod
    def eval_atomic(cond, json_tuple):
        '''Evaluate an atomic condition (comparison) against a JSON tuple.'''
        left, right = cond.inputs
        op = cond.op
        
        if isinstance(left, radb.ast.AttrRef):
            attr, literal = left, right
        elif isinstance(right, radb.ast.AttrRef):
            attr, literal = right, left
        else:
            return False
        
        attr_name = attr.name
        attr_rel = attr.rel
        
        if isinstance(literal, radb.ast.RAString):
            literal_val = literal.val.strip("'").strip('"')
        elif isinstance(literal, radb.ast.RANumber):
            literal_val = literal.val
        else:
            return False
        
        if attr_rel is not None:
            key = f"{attr_rel}.{attr_name}"
            if key not in json_tuple:
                return False
        else:
            key = next(
                (k for k in json_tuple if k.endswith(f".{attr_name}")),
                None
            )
            if key is None:
                return False
        
        tuple_val = json_tuple[key]
        
        try:
            if isinstance(tuple_val, int):
                literal_val = int(literal_val)
            elif isinstance(tuple_val, float):
                literal_val = float(literal_val)
            else:
                literal_val = str(literal_val)
        except Exception:
            return False
        
        if op == radb.ast.sym.EQ:
            return tuple_val == literal_val
        if op == radb.ast.sym.LT:
            return tuple_val < literal_val
        if op == radb.ast.sym.GT:
            return tuple_val > literal_val
        if op == radb.ast.sym.LE:
            return tuple_val <= literal_val
        if op == radb.ast.sym.GE:
            return tuple_val >= literal_val
        
        return False
    
    @staticmethod
    def apply_selection(condition, json_tuple):
        return MapperHelper.eval_condition(condition, json_tuple)

    @staticmethod
    def apply_rename(old_rel, new_rel, json_tuple):
        new_tuple = {}
        for key, value in json_tuple.items():
            if key.startswith(old_rel + "."):
                new_key = new_rel + key[len(old_rel):]
                new_tuple[new_key] = value
            else:
                new_tuple[key] = value
        return new_tuple
    
    @staticmethod
    def apply_projection(attrs, json_tuple):
        projected_tuple = {}
        for attr in attrs:
            attr_name = attr.name
            attr_rel = attr.rel
            
            for key, value in json_tuple.items():
                if attr_rel is not None:
                    if key == f"{attr_rel}.{attr_name}":
                        projected_tuple[key] = value
                else:
                    if key.endswith(f".{attr_name}"):
                        projected_tuple[key] = value
        
        return projected_tuple
    
    @staticmethod
    def extract_equality_attrs(cond):
        eq_attrs = []
        
        if isinstance(cond, radb.ast.ValExprBinaryOp):
            if cond.op == radb.ast.sym.EQ:
                eq_attrs.append((cond.inputs[0], cond.inputs[1]))
            elif cond.op == radb.ast.sym.AND:
                eq_attrs.extend(MapperHelper.extract_equality_attrs(cond.inputs[0]))
                eq_attrs.extend(MapperHelper.extract_equality_attrs(cond.inputs[1]))
        
        return eq_attrs
    
    @staticmethod
    def build_join_key(eq_attrs, json_tuple):
        join_key_list = []
        
        for left_attr, right_attr in eq_attrs:
            key_left = f"{left_attr.rel}.{left_attr.name}"
            key_right = f"{right_attr.rel}.{right_attr.name}"
            
            if key_left in json_tuple:
                join_key_list.append(json_tuple[key_left])
            elif key_right in json_tuple:
                join_key_list.append(json_tuple[key_right])
            else:
                key_found = next((k for k in json_tuple if k.endswith(f".{left_attr.name}")), None)
                if key_found:
                    join_key_list.append(json_tuple[key_found])
                else:
                    key_found = next((k for k in json_tuple if k.endswith(f".{right_attr.name}")), None)
                    if key_found:
                        join_key_list.append(json_tuple[key_found])
                    else:
                        return None
        
        if join_key_list:
            return "|".join(str(v) for v in join_key_list)
        return None

    @staticmethod
    def process_node(node, tuple_in, relation_name):
        if isinstance(node, SourceNode):
            if rel_id(node.rel) == relation_name:
                return tuple_in, None
            return None
        elif isinstance(node, MapChainNode):
            current_tuple = tuple_in
            for op in node.ops:
                if isinstance(op, radb.ast.Select):
                    if not MapperHelper.apply_selection(op.cond, current_tuple):
                        return None
                elif isinstance(op, radb.ast.Rename):
                    if current_tuple:
                        first_key = next(iter(current_tuple.keys()))
                        old_rel = first_key.split('.')[0] if '.' in first_key else ""
                    else:
                        old_rel = ""
                    new_rel = op.relname
                    current_tuple = MapperHelper.apply_rename(old_rel, new_rel, current_tuple)
                elif isinstance(op, radb.ast.Project):
                    current_tuple = MapperHelper.apply_projection(op.attrs, current_tuple)
            return MapperHelper.process_node(node.child, current_tuple, relation_name)
        elif isinstance(node, JoinNode):
            left_res = MapperHelper.process_node(node.left, tuple_in, relation_name)
            if left_res:
                return left_res[0], "L"
            right_res = MapperHelper.process_node(node.right, tuple_in, relation_name)
            if right_res:
                return right_res[0], "R"
            return None
        else:
            raise NotImplementedError(type(node))

class JoinTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Join))

        task1 = task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment, optimize=self.optimize)
        task2 = task_factory(raquery.inputs[1], step=self.step + count_steps(raquery.inputs[0]) + 1,
                             env=self.exec_environment, optimize=self.optimize)

        return [task1, task2]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        raquery = radb.parse.one_statement_from_string(self.querystring)
        condition = raquery.cond

        eq_attrs = MapperHelper.extract_equality_attrs(condition)
        join_key = MapperHelper.build_join_key(eq_attrs, json_tuple)
        
        if join_key:
            yield (join_key, (relation, json_tuple))

    def reducer(self, key, values):
        raquery = radb.parse.one_statement_from_string(self.querystring)

        values = list(values)
        out_rel = rel_id(raquery)

        rels = list({rel for rel, _ in values})
        if len(rels) != 2:
            return

        left_rel, right_rel = rels
        left_tuples = [v for rel, v in values if rel == left_rel]
        right_tuples = [v for rel, v in values if rel == right_rel]

        for lt in left_tuples:
            for rt in right_tuples:
                joined = {**lt, **rt}
                yield (out_rel, json.dumps(joined))



class SelectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Select))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment, optimize=self.optimize)]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        condition = radb.parse.one_statement_from_string(self.querystring).cond
        if MapperHelper.apply_selection(condition, json_tuple):
            yield (relation, json.dumps(json_tuple))

class RenameTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Rename))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment, optimize=self.optimize)]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        raquery = radb.parse.one_statement_from_string(self.querystring)
        old_rel = raquery.inputs[0].rel if isinstance(raquery.inputs[0], radb.ast.RelRef) else relation
        new_rel = raquery.relname

        new_tuple = MapperHelper.apply_rename(old_rel, new_rel, json_tuple)
        yield (rel_id(raquery), json.dumps(new_tuple))

class ProjectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Project))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment, optimize=self.optimize)]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        attrs = radb.parse.one_statement_from_string(self.querystring).attrs

        projected_tuple = MapperHelper.apply_projection(attrs, json_tuple)
        
        if projected_tuple:
            yield (relation, json.dumps(projected_tuple))


    def reducer(self, key, values):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        seen = set()
        for v in values:
            if v not in seen:
                seen.add(v)
                yield (rel_id(raquery), v)

class MapperChainTask(RelAlgQueryTask):
    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        node = raquery

        while isinstance(node, (radb.ast.Select, radb.ast.Project, radb.ast.Rename)):
            node = node.inputs[0]

        return [task_factory(node, step=self.step + 1, env=self.exec_environment, optimize=self.optimize)]

    def mapper(self, line):
        relation, tuple_str = line.split('\t')
        json_tuple = json.loads(tuple_str)

        raquery = radb.parse.one_statement_from_string(self.querystring)
        ops = []
        node = raquery
        while isinstance(node, (radb.ast.Select, radb.ast.Project, radb.ast.Rename)):
            ops.append(node)
            node = node.inputs[0]

        ops.reverse()

        current_tuple = json_tuple
        current_relation = relation

        for op in ops:
            if isinstance(op, radb.ast.Select):
                if not MapperHelper.apply_selection(op.cond, current_tuple):
                    return
            elif isinstance(op, radb.ast.Rename):
                old_rel = op.inputs[0].rel if hasattr(op.inputs[0], "rel") else current_relation
                new_rel = op.relname
                current_tuple = MapperHelper.apply_rename(old_rel, new_rel, current_tuple)
                current_relation = new_rel
            elif isinstance(op, radb.ast.Project):
                current_tuple = MapperHelper.apply_projection(op.attrs, current_tuple)

        if current_tuple:
            yield (rel_id(raquery), json.dumps(current_tuple))

    def reducer(self, key, values):
        raquery = radb.parse.one_statement_from_string(self.querystring)

        seen = set()
        for v in values:
            if v not in seen:
                seen.add(v)
                yield (rel_id(raquery), v)

class EnhancedJoinTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        self.plan = build_execution_plan(raquery)

        def find_boundary(node):
            if isinstance(node, (radb.ast.Select, radb.ast.Project, radb.ast.Rename)):
                return find_boundary(node.inputs[0])
            return node

        left_boundary = find_boundary(raquery.inputs[0])
        right_boundary = find_boundary(raquery.inputs[1])

        task1 = task_factory(left_boundary, step=self.step + 1, env=self.exec_environment, optimize=self.optimize)
        task2 = task_factory(right_boundary, step=self.step + count_steps(raquery.inputs[0]) + 1,
                             env=self.exec_environment, optimize=self.optimize)

        if task1 == task2:
            return [task1]
        return [task1, task2]

    def mapper(self, line):
        relation, tuple_str = line.split('\t')
        json_tuple = json.loads(tuple_str)

        eq_attrs = MapperHelper.extract_equality_attrs(self.plan.condition)
        left_result = MapperHelper.process_node(self.plan.left, json_tuple, relation)
        if left_result:
            tuple_after_ops, _ = left_result
            join_key = MapperHelper.build_join_key(eq_attrs, tuple_after_ops)
            if join_key:
                yield (join_key, ("L", tuple_after_ops))
        
        right_result = MapperHelper.process_node(self.plan.right, json_tuple, relation)
        if right_result:
            tuple_after_ops, _ = right_result
            join_key = MapperHelper.build_join_key(eq_attrs, tuple_after_ops)
            if join_key:
                yield (join_key, ("R", tuple_after_ops))

    def reducer(self, key, values):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        left = []
        right = []
        for side, tup in values:
            if side == "L":
                left.append(tup)
            else:
                right.append(tup)
        for l in left:
            for r in right:
                joined = {**l, **r}
                yield (rel_id(raquery), json.dumps(joined))


if __name__ == '__main__':
    luigi.run()
