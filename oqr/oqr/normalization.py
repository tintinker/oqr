from dataclasses import dataclass
from typing import List, Union, Dict, Any


@dataclass
class LogicalNode:
    op: str
    children: List["NodeType"]


@dataclass
class ComparisonNode:
    field: str
    op: str
    value: Any


@dataclass
class ExistsNode:
    field: str


@dataclass
class MatchAllNode:
    pass


NodeType = Union[LogicalNode, ComparisonNode, ExistsNode, MatchAllNode]


def normalized_readable_query(query: Dict[str, Any]) -> str:
    root = _query_to_tree(query)
    normalized_root = _normalize_tree(root)
    reduced_root = _reduce_double_negation(normalized_root)
    flattened_root = flatten_logical_nodes(reduced_root)
    final = _tree_to_string(flattened_root)
    return final


def _query_to_tree(query: Dict[str, Any]) -> NodeType:
    if not isinstance(query, dict):
        return query

    if "query" in query:
        query = query["query"]

    if "bool" in query:
        bool_query = query["bool"]
        children = []

        if "must" in bool_query:
            children.extend(_query_to_tree(clause) for clause in bool_query["must"])
        if "should" in bool_query:
            children.extend(_query_to_tree(clause) for clause in bool_query["should"])
        if "must_not" in bool_query:
            children.extend(
                LogicalNode("not", [_query_to_tree(clause)])
                for clause in bool_query["must_not"]
            )

        if len(children) == 1:
            return children[0]
        return LogicalNode("and" if "must" in bool_query else "or", children)

    if "terms" in query:
        field, values = next(iter(query["terms"].items()))
        return LogicalNode(
            "or", [ComparisonNode(field, "==", value) for value in values]
        )

    if "range" in query:
        field, condition = next(iter(query["range"].items()))
        op, value = next(iter(condition.items()))
        return ComparisonNode(field, op, value)

    if "exists" in query:
        return ExistsNode(query["exists"]["field"])

    if "match_all" in query:
        return MatchAllNode()

    if "term" in query:
        field, value = next(iter(query["term"].items()))
        return ComparisonNode(field, "==", value)

    raise ValueError(f"Unsupported query type: {query}")


def _normalize_tree(node: NodeType) -> NodeType:
    if isinstance(node, LogicalNode):
        node.children = [_normalize_tree(child) for child in node.children]

        if node.op == "not":
            child = node.children[0]
            if isinstance(child, LogicalNode):
                if child.op == "and":
                    return LogicalNode(
                        "or",
                        [
                            LogicalNode("not", [_normalize_tree(c)])
                            for c in child.children
                        ],
                    )
                elif child.op == "or":
                    return LogicalNode(
                        "and",
                        [
                            LogicalNode("not", [_normalize_tree(c)])
                            for c in child.children
                        ],
                    )
            elif isinstance(child, ComparisonNode):
                if child.op == ">":
                    return ComparisonNode(child.field, "<=", child.value)
                elif child.op == ">=":
                    return ComparisonNode(child.field, "<", child.value)
                elif child.op == "!=":
                    return ComparisonNode(child.field, "==", child.value)

        return node
    elif isinstance(node, ComparisonNode):
        if node.op in ("gt", ">"):
            return LogicalNode("not", [ComparisonNode(node.field, "<=", node.value)])
        elif node.op in ("gte", ">="):
            return LogicalNode("not", [ComparisonNode(node.field, "<", node.value)])
        elif node.op in ("lt", "<"):
            return ComparisonNode(node.field, "<", node.value)
        elif node.op in ("lte", "<="):
            return ComparisonNode(node.field, "<=", node.value)
        return node
    else:
        return node


def _reduce_double_negation(node: NodeType) -> NodeType:
    if isinstance(node, LogicalNode):
        node.children = [_reduce_double_negation(child) for child in node.children]

        if (
            node.op == "not"
            and isinstance(node.children[0], LogicalNode)
            and node.children[0].op == "not"
        ):
            return _reduce_double_negation(node.children[0].children[0])

        return node
    else:
        return node


def flatten_logical_nodes(node: NodeType) -> NodeType:
    if isinstance(node, LogicalNode):
        flattened_children = []
        for child in node.children:
            flattened_child = flatten_logical_nodes(child)
            if (
                isinstance(flattened_child, LogicalNode)
                and flattened_child.op == node.op
            ):
                flattened_children.extend(flattened_child.children)
            else:
                flattened_children.append(flattened_child)
        return LogicalNode(node.op, flattened_children)
    return node


def _tree_to_string(node: NodeType) -> str:
    if isinstance(node, LogicalNode):
        children_strs = sorted([_tree_to_string(child) for child in node.children])
        children_str = ", ".join(children_strs)
        return f"{node.op}({children_str})"
    elif isinstance(node, ComparisonNode):
        return f"{node.field} {node.op} {node.value}"
    elif isinstance(node, ExistsNode):
        return f"exists({node.field})"
    elif isinstance(node, MatchAllNode):
        return "match_all()"
    else:
        return str(node)
