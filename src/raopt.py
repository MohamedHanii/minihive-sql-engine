"""Logical optimization rules for relational algebra (radb) query trees."""
import radb.ast as ast

# ================== Helper Methods =========================
def split_predicates(cond):
    if isinstance(cond, ast.ValExprBinaryOp) and cond.op == ast.sym.AND:
        return split_predicates(cond.inputs[0]) + split_predicates(cond.inputs[1])
    return [cond]

def belongs_to_table_or_constant(side, dd, node_rel, original_rel=None):
    if isinstance(side, ast.AttrRef):
        if side.rel is not None:
            allowed_tables = {node_rel}
            if original_rel is not None:
                allowed_tables.add(original_rel)
            return side.rel in allowed_tables
        else:
            tables_to_check = [node_rel]
            if original_rel:
                tables_to_check.append(original_rel)
            for table in tables_to_check:
                if table in dd and side.name in dd[table]:
                    return True
            return False

    if isinstance(side, ast.ValExpr):
        return True

    return False

def relations_in_subtree(node):
    if isinstance(node, ast.RelRef):
        return {node.rel}

    if isinstance(node, ast.Rename):
        return {node.relname}

    if hasattr(node, "inputs"):
        rels = set()
        for child in node.inputs:
            rels |= relations_in_subtree(child)
        return rels

    return set()

def tables_for_attr(side,dd):
    if isinstance(side, ast.AttrRef):
        if side.rel:
            return {side.rel}  
        possible_tables = set()
        for table, cols in dd.items():
            if side.name in cols:
                possible_tables.add(table)

        return possible_tables

    return set()

def extract_attributes(node):
    if isinstance(node, ast.AttrRef):
        return {node}
    if hasattr(node, "inputs"):
        attrs = set()
        for child in node.inputs:
            attrs |= extract_attributes(child)
        return attrs
    return set()

def get_all_columns(rel, dd):
    if rel in dd:
        return {ast.AttrRef(rel, col) for col in dd[rel]}
    return set()

def handle_select_predicate(node, dd, selection_list):
    selection_list.append(node.cond)
    child = push_down_with_set(node.inputs[0], dd, selection_list)
    node.inputs[0] = child

    if node.cond not in selection_list:
        return child
    return node

def handle_rename_predicate(node, dd, selection_list):
    original_rel = node.inputs[0].rel
    node_rel = node.relname
    dd[node_rel] = dd[original_rel].copy()
    to_apply = []

    for cond in selection_list:
        left, right = None, None
        if isinstance(cond, ast.ValExprBinaryOp):
            left, right = cond.inputs
        else:
            continue

        if belongs_to_table_or_constant(left, dd, node_rel, original_rel) and belongs_to_table_or_constant(right, dd, node_rel, original_rel):
            to_apply.append(cond)

    for cond in reversed(to_apply):
        node = ast.Select(cond, node)
        selection_list.remove(cond)

    inner_selection_list = []
    node.inputs[0] = push_down_with_set(node.inputs[0], dd, inner_selection_list)
    return node


def handle_entity(node, dd, selection_list):
    to_apply = []

    for cond in selection_list:
        left, right = None, None
        if isinstance(cond, ast.ValExprBinaryOp):
            left, right = cond.inputs
        else:
            continue

        if belongs_to_table_or_constant(left, dd, node.rel) and belongs_to_table_or_constant(right, dd, node.rel):
            to_apply.append(cond)

    for cond in reversed(to_apply):
        node = ast.Select(cond, node)
        selection_list.remove(cond)

    return node

# ============================= Main Methods =============================

# -------------------------------
# Break Up Selections
# -------------------------------
def traverse_break(node):
    if isinstance(node, ast.Select):
        preds = split_predicates(node.cond)
        child = traverse_break(node.inputs[0])

        for p in reversed(preds):
            child = ast.Select(p, child)

        return child

  
    if hasattr(node, "inputs") and len(node.inputs) == 1:
        node.inputs[0] = traverse_break(node.inputs[0])
        return node

 
    if hasattr(node, "inputs") and len(node.inputs) == 2:
        node.inputs[0] = traverse_break(node.inputs[0])
        node.inputs[1] = traverse_break(node.inputs[1])
        return node

    return node

# -------------------------------
# Push Down Selections
# -------------------------------
def push_down_with_set(node, dd, selection_list=None):
    if selection_list is None:
        selection_list = []

    if isinstance(node, ast.Select):
        return handle_select_predicate(node,dd,selection_list)

    if isinstance(node, ast.Rename):
        return handle_rename_predicate(node,dd,selection_list)


    if isinstance(node, ast.RelRef):
        return handle_entity(node, dd, selection_list)


    if hasattr(node, "inputs") and len(node.inputs) == 1:
        node.inputs[0] = push_down_with_set(node.inputs[0], dd, selection_list)
        return node

    if hasattr(node, "inputs") and len(node.inputs) == 2:
        left = push_down_with_set(node.inputs[0], dd, selection_list)
        right = push_down_with_set(node.inputs[1], dd, selection_list)
        node.inputs[0] = left
        node.inputs[1] = right

        for cond in reversed(selection_list):
            if not isinstance(cond, ast.ValExprBinaryOp):
                continue
            l, r = cond.inputs

            needed_tables = tables_for_attr(l, dd) | tables_for_attr(r, dd)
            available_tables = relations_in_subtree(left) | relations_in_subtree(right)

            if needed_tables <= available_tables:
                node = ast.Select(cond, node)
                selection_list.remove(cond)

        return node

    return node


# -------------------------------
# Push Down Projections
# -------------------------------
def push_down_project(node, required_attrs, dd):
    if isinstance(node, ast.Select):
        if required_attrs is None:
            new_required = None
        else:
            new_required = required_attrs | extract_attributes(node.cond)
        node.inputs[0] = push_down_project(node.inputs[0], new_required, dd)
        return node

    if isinstance(node, ast.Project):
        node.inputs[0] = push_down_project(node.inputs[0], set(node.attrs), dd)
        return node.inputs[0]


    if isinstance(node, ast.Rename):
        child = node.inputs[0]

        if required_attrs is None:
            new_required = None
        else:

            new_required = set()
            for attr in required_attrs:
                if isinstance(attr, ast.AttrRef):
                    if attr.rel == node.relname:

                        if isinstance(child, ast.RelRef):
                            new_required.add(ast.AttrRef(child.rel, attr.name))
                        else:

                            new_required.add(ast.AttrRef(None, attr.name))
                    else:
                        new_required.add(attr)
                else:
                    new_required.add(attr)

        node.inputs[0] = push_down_project(child, new_required, dd)
        return node

    if isinstance(node, ast.Join) or isinstance(node, ast.Cross):
        left_rels = relations_in_subtree(node.inputs[0])
        right_rels = relations_in_subtree(node.inputs[1])

        join_attrs = set()
        if isinstance(node, ast.Join):
            join_attrs = extract_attributes(node.cond)

        if required_attrs is None:
            left_required = None
            right_required = None
        else:
            left_required = set()
            right_required = set()

            for attr in required_attrs | join_attrs:
                if not isinstance(attr, ast.AttrRef):
                    continue

                if attr.rel in left_rels:
                    left_required.add(attr)
                elif attr.rel in right_rels:
                    right_required.add(attr)
                else:
                    for rel in left_rels:
                        if rel in dd and attr.name in dd[rel]:
                            left_required.add(ast.AttrRef(rel, attr.name))
                    for rel in right_rels:
                        if rel in dd and attr.name in dd[rel]:
                            right_required.add(ast.AttrRef(rel, attr.name))

        node.inputs[0] = push_down_project(node.inputs[0], left_required, dd)
        node.inputs[1] = push_down_project(node.inputs[1], right_required, dd)
        return node

    if isinstance(node, ast.RelRef):
        if required_attrs is None or not required_attrs:
            all_attrs = get_all_columns(node.rel, dd)
            if all_attrs:
                return ast.Project(list(all_attrs), node)
            return node

        rel_required = []
        for attr in required_attrs:
            if not isinstance(attr, ast.AttrRef):
                continue
            if attr.rel in (None, node.rel):
                if node.rel in dd and attr.name in dd[node.rel]:
                    rel_required.append(ast.AttrRef(node.rel, attr.name))

        if rel_required:
            return ast.Project(rel_required, node)
        return node

    if hasattr(node, "inputs"):
        for i in range(len(node.inputs)):
            node.inputs[i] = push_down_project(node.inputs[i], required_attrs, dd)
        return node

    return node


def rule_push_down_projections(ra, dd):
    if isinstance(ra, ast.Project):
        attrs = set(ra.attrs)
        ra.inputs[0] = push_down_project(ra.inputs[0], attrs, dd)
        return ra
    else:
        return push_down_project(ra, None, dd)

def rule_break_up_selections(ra):
    return traverse_break(ra)

def rule_push_down_selections(ra,dd):
    return push_down_with_set(ra,dd)

def rule_merge_selections(ra):
    if isinstance(ra, ast.Select):
        if isinstance(ra.inputs[0], ast.Select):
            merged_cond = ast.ValExprBinaryOp(ra.cond, ast.sym.AND, ra.inputs[0].cond)
            return rule_merge_selections(ast.Select(merged_cond, ra.inputs[0].inputs[0]))
        else:
            return ast.Select(ra.cond, rule_merge_selections(ra.inputs[0]))
    elif isinstance(ra, ast.Project):
        return ast.Project(ra.attrs, rule_merge_selections(ra.inputs[0]))
    elif isinstance(ra, ast.Rename):
        return ast.Rename(ra.relname, ra.attrnames, rule_merge_selections(ra.inputs[0]))
    elif isinstance(ra, ast.Cross):
        return ast.Cross(rule_merge_selections(ra.inputs[0]), rule_merge_selections(ra.inputs[1]))
    else:
        return ra

def rule_introduce_joins(ra):
    if isinstance(ra, ast.Select):
        inner = ra.inputs[0]
        if isinstance(inner, ast.Cross):
            cond = ra.cond
            return ast.Join(rule_introduce_joins(inner.inputs[0]),
                            cond,
                            rule_introduce_joins(inner.inputs[1]))
        else:
            return ast.Select(ra.cond, rule_introduce_joins(inner))

    elif isinstance(ra, ast.Project):
        return ast.Project(ra.attrs, rule_introduce_joins(ra.inputs[0]))
    elif isinstance(ra, ast.Rename):
        return ast.Rename(ra.relname, ra.attrnames, rule_introduce_joins(ra.inputs[0]))
    elif isinstance(ra, ast.Cross):
        return ast.Cross(rule_introduce_joins(ra.inputs[0]), rule_introduce_joins(ra.inputs[1]))
    else:
        return ra
