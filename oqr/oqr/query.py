from typing import Any, Dict, List, Optional, Union


class QueryCondition:
    def __init__(
        self,
        field: str = None,
        value: Any = None,
        operator: str = None,
        boost: float = None,
    ) -> None:
        self.field = field
        self.value = value
        self.operator = operator
        self.boost = boost

    def _build_query(self) -> dict:
        """Build the corresponding OpenSearch query."""
        if self.operator == "term":
            return {"term": {self.field: self.value}}
        elif self.operator == "range":
            return {"range": {self.field: self.value}}
        elif self.operator == "terms":
            return {"terms": {self.field: self.value}}
        elif self.operator == "exists":
            return {"exists": {"field": self.field}}
        elif self.operator == "bool":
            return {self.operator: self.value}
        elif self.operator == "match_all":
            return {"match_all": {}}
        else:
            raise ValueError(f"Unsupported operator: {self.operator}")

    def __and__(
        self,
        other: Union[
            "QueryCondition", "Limit", "ScriptScore", "QueryConditionWithScoring"
        ],
    ) -> Union["QueryCondition", "QueryConditionWithScoring"]:
        """Handle AND operation (&)."""
        if (
            isinstance(other, Limit)
            or isinstance(other, ScriptScore)
            or isinstance(other, QueryConditionWithScoring)
        ):
            return other & self

        return Query.and_(self, other)

    def __or__(
        self,
        other: Union[
            "QueryCondition", "Limit", "ScriptScore", "QueryConditionWithScoring"
        ],
    ) -> Union["QueryCondition", "QueryConditionWithScoring"]:
        """Handle OR operation (|)."""
        if (
            isinstance(other, Limit)
            or isinstance(other, ScriptScore)
            or isinstance(other, QueryConditionWithScoring)
        ):
            return other | self
        return Query.or_(self, other)

    def __invert__(self) -> "QueryCondition":
        """Handle NOT operation (~)."""
        return Query.not_(self)

    def __lt__(self, other: Union[int, float]) -> "QueryCondition":
        return QueryCondition(field=self.field, value={"lt": other}, operator="range")

    def __gt__(self, other: Union[int, float]) -> "QueryCondition":
        return QueryCondition(field=self.field, value={"gt": other}, operator="range")

    def __le__(self, other: Union[int, float]) -> "QueryCondition":
        return QueryCondition(field=self.field, value={"lte": other}, operator="range")

    def __ge__(self, other: Union[int, float]) -> "QueryCondition":
        return QueryCondition(field=self.field, value={"gte": other}, operator="range")

    def __eq__(self, other: Union[int, float, str]) -> "QueryCondition":
        """
        Handle equality (==), raise error for unsupported types.
        If the value is float or int, build an OR query to support both numeric types in the index.
        """
        if isinstance(
            other, (int, float)
        ):  # For numerical comparison, create an OR query to handle both float and long types
            return Query.or_(
                QueryCondition(field=self.field, value=float(other), operator="term"),
                QueryCondition(field=self.field, value=int(other), operator="term"),
            )
        elif isinstance(other, str):  # For string comparison
            return QueryCondition(field=self.field, value=other, operator="term")
        else:
            raise TypeError(
                f"Unsupported type for equality comparison: {type(other)}. Must be int, float, or str."
            )

    def __ne__(self, other: Union[int, float, str]) -> "QueryCondition":
        """Handle inequality (!=)."""
        return Query.not_(self.__eq__(other))

    def isin(self, values: List[Any]) -> "QueryCondition":
        """Custom method for handling `isin` operator (equivalent to `terms` in OpenSearch)."""
        return QueryCondition(field=self.field, value=values, operator="terms")

    def exists(self) -> "QueryCondition":
        """Custom method for handling `exists` operator."""
        return QueryCondition(field=self.field, value=None, operator="exists")

    def to_dict(self) -> dict:
        """Convert the final QueryCondition to an OpenSearch query dict."""

        return self._build_query()

    def __repr__(self) -> str:
        """Automatically return the OpenSearch query as a dictionary when queried or printed."""
        return str(self.to_dict())


class Limit:
    def __init__(self, limit: int) -> None:
        self.limit = limit

    def __and__(
        self, other: Union["Limit", "QueryCondition", "QueryConditionWithScoring"]
    ) -> Union["Limit", "QueryConditionWithScoring"]:
        if isinstance(other, Limit):
            return Limit(min(self.limit, other.limit))
        elif isinstance(other, QueryCondition):
            return QueryConditionWithScoring(other, limit=self)
        elif isinstance(other, QueryConditionWithScoring):
            return other & self
        else:
            raise TypeError(
                f"Unsupported type for & operation with Limit: {type(other)}"
            )

    def __or__(
        self, other: Union["Limit", "QueryCondition", "QueryConditionWithScoring"]
    ) -> Union["Limit", "QueryConditionWithScoring"]:
        if isinstance(other, Limit):
            return Limit(max(self.limit, other.limit))
        elif isinstance(other, QueryCondition):
            return QueryConditionWithScoring(other, limit=self)
        elif isinstance(other, QueryConditionWithScoring):
            return other | self
        else:
            raise TypeError(
                f"Unsupported type for | operation with Limit: {type(other)}"
            )

    def __repr__(self) -> str:
        return f"Limit({self.limit})"


class ScriptScore:
    def __init__(self, script: str, params: Dict[str, Any] = None) -> None:
        self.script = script
        self.params = params or {}

    def __and__(
        self, other: Union["QueryCondition", "QueryConditionWithScoring"]
    ) -> "QueryConditionWithScoring":
        if isinstance(other, QueryCondition):
            return QueryConditionWithScoring(other, and_scorers=[self])
        elif isinstance(other, QueryConditionWithScoring):
            return other & self
        else:
            raise TypeError(
                f"Unsupported type for & operation with ScriptScore: {type(other)}"
            )

    def __or__(
        self, other: Union["QueryCondition", "QueryConditionWithScoring"]
    ) -> "QueryConditionWithScoring":
        if isinstance(other, QueryCondition):
            return QueryConditionWithScoring(other, or_scorers=[self])
        elif isinstance(other, QueryConditionWithScoring):
            return other | self
        else:
            raise TypeError(
                f"Unsupported type for | operation with ScriptScore: {type(other)}"
            )

    def __repr__(self) -> str:
        return f"ScriptScore(script='{self.script}', params={self.params})"


class QueryConditionWithScoring:
    def __init__(
        self,
        condition: QueryCondition,
        and_scorers: Optional[List[ScriptScore]] = None,
        or_scorers: Optional[List[ScriptScore]] = None,
        limit: Optional[Limit] = None,
    ) -> None:
        self.condition = condition
        self.and_scorers = and_scorers or []
        self.or_scorers = or_scorers or []
        self.limit = limit
        if self.and_scorers and self.or_scorers:
            raise ValueError(
                "Cannot have both 'and_scorers' and 'or_scorers' at the same time."
            )

    def __and__(
        self,
        other: Union[QueryCondition, ScriptScore, Limit, "QueryConditionWithScoring"],
    ) -> "QueryConditionWithScoring":
        if isinstance(other, QueryCondition):
            return QueryConditionWithScoring(
                Query.and_(self.condition, other),
                and_scorers=self.and_scorers,
                or_scorers=self.or_scorers,
                limit=self.limit,
            )
        elif isinstance(other, ScriptScore):
            if self.or_scorers:
                raise ValueError(
                    "Cannot add 'and_scorers' when 'or_scorers' already exist."
                )
            return QueryConditionWithScoring(
                self.condition,
                and_scorers=self.and_scorers + [other],
                or_scorers=self.or_scorers,
                limit=self.limit,
            )
        elif isinstance(other, Limit):
            return QueryConditionWithScoring(
                self.condition,
                and_scorers=self.and_scorers,
                or_scorers=self.or_scorers,
                limit=(
                    other
                    if self.limit is None
                    else Limit(min(self.limit.limit, other.limit))
                ),
            )
        elif isinstance(other, QueryConditionWithScoring):
            combined_condition = Query.and_(self.condition, other.condition)
            combined_and_scorers = self.and_scorers + other.and_scorers
            combined_or_scorers = self.or_scorers + other.or_scorers
            if combined_and_scorers and combined_or_scorers:
                raise ValueError(
                    "Cannot have both 'and_scorers' and 'or_scorers' at the same time."
                )
            combined_limit = (
                Limit(min(self.limit.limit, other.limit.limit))
                if self.limit and other.limit
                else (self.limit or other.limit)
            )
            return QueryConditionWithScoring(
                combined_condition,
                and_scorers=combined_and_scorers,
                or_scorers=combined_or_scorers,
                limit=combined_limit,
            )
        else:
            raise TypeError(f"Unsupported type for & operation: {type(other)}")

    def __or__(
        self,
        other: Union[QueryCondition, ScriptScore, Limit, "QueryConditionWithScoring"],
    ) -> "QueryConditionWithScoring":
        if isinstance(other, QueryCondition):
            return QueryConditionWithScoring(
                Query.or_(self.condition, other),
                and_scorers=self.and_scorers,
                or_scorers=self.or_scorers,
                limit=self.limit,
            )
        elif isinstance(other, ScriptScore):
            if self.and_scorers:
                raise ValueError(
                    "Cannot add 'or_scorers' when 'and_scorers' already exist."
                )
            return QueryConditionWithScoring(
                self.condition,
                and_scorers=self.and_scorers,
                or_scorers=self.or_scorers + [other],
                limit=self.limit,
            )
        elif isinstance(other, Limit):
            return QueryConditionWithScoring(
                self.condition,
                and_scorers=self.and_scorers,
                or_scorers=self.or_scorers,
                limit=(
                    other
                    if self.limit is None
                    else Limit(max(self.limit.limit, other.limit))
                ),
            )
        elif isinstance(other, QueryConditionWithScoring):
            combined_condition = Query.or_(self.condition, other.condition)
            combined_and_scorers = self.and_scorers + other.and_scorers
            combined_or_scorers = self.or_scorers + other.or_scorers
            if combined_and_scorers and combined_or_scorers:
                raise ValueError(
                    "Cannot have both 'and_scorers' and 'or_scorers' at the same time."
                )
            combined_limit = (
                Limit(max(self.limit.limit, other.limit.limit))
                if self.limit and other.limit
                else (self.limit or other.limit)
            )
            return QueryConditionWithScoring(
                combined_condition,
                and_scorers=combined_and_scorers,
                or_scorers=combined_or_scorers,
                limit=combined_limit,
            )
        else:
            raise TypeError(f"Unsupported type for | operation: {type(other)}")

    def __invert__(self) -> "QueryConditionWithScoring":
        return QueryConditionWithScoring(
            Query.not_(self.condition),
            and_scorers=self.and_scorers,
            or_scorers=self.or_scorers,
            limit=self.limit,
        )

    def to_dict(self) -> dict:
        query = self.condition.to_dict()
        if self.and_scorers or self.or_scorers:
            function_score = {"query": query, "functions": []}
            if self.and_scorers:
                for scorer in self.and_scorers:
                    function_score["functions"].append(
                        {
                            "script_score": {
                                "script": {
                                    "source": scorer.script,
                                    "params": scorer.params,
                                }
                            }
                        }
                    )
                function_score["score_mode"] = "multiply"
            elif self.or_scorers:
                for scorer in self.or_scorers:
                    function_score["functions"].append(
                        {
                            "script_score": {
                                "script": {
                                    "source": scorer.script,
                                    "params": scorer.params,
                                }
                            }
                        }
                    )
                function_score["score_mode"] = "max"
            query = {"function_score": function_score}
        if self.limit:
            return {"size": self.limit.limit, "query": query}
        return {"query": query}

    def __repr__(self) -> str:
        return f"QueryConditionWithScoring(condition={self.condition}, and_scorers={self.and_scorers}, or_scorers={self.or_scorers}, limit={self.limit})"


class Query:
    @staticmethod
    def _validate_conditions(conditions: List[QueryCondition]) -> None:
        """Validate that the input is a flat list of QueryCondition instances."""
        for i, cond in enumerate(conditions):
            if not isinstance(cond, QueryCondition):
                raise TypeError(
                    f"Invalid input at index {i}: Expected QueryCondition, got {type(cond).__name__}."
                )

    @staticmethod
    def and_(
        *conditions: Union[QueryCondition, List[QueryCondition]]
    ) -> QueryCondition:
        """Combine multiple conditions with AND (must), supports both *list and list."""
        # If a single list is passed, unpack it
        if len(conditions) == 1 and isinstance(conditions[0], list):
            conditions = conditions[0]

        # Validate that the conditions are a flat list of QueryCondition
        Query._validate_conditions(conditions)

        return QueryCondition(
            operator="bool", value={"must": [cond.to_dict() for cond in conditions]}
        )

    @staticmethod
    def or_(*conditions: Union[QueryCondition, List[QueryCondition]]) -> QueryCondition:
        """Combine multiple conditions with OR (should), supports both *list and list."""
        # If a single list is passed, unpack it
        if len(conditions) == 1 and isinstance(conditions[0], list):
            conditions = conditions[0]

        # Validate that the conditions are a flat list of QueryCondition
        Query._validate_conditions(conditions)

        return QueryCondition(
            operator="bool", value={"should": [cond.to_dict() for cond in conditions]}
        )

    @staticmethod
    def not_(condition: QueryCondition) -> QueryCondition:
        """Negate a condition (must_not)."""
        if not isinstance(condition, QueryCondition):
            raise TypeError(
                f"Invalid condition for NOT operation: Expected QueryCondition, got {type(condition).__name__}."
            )
        return QueryCondition(
            operator="bool", value={"must_not": [condition.to_dict()]}
        )

    @staticmethod
    def with_scoring(
        condition: QueryCondition,
        and_scorers: List[ScriptScore] = None,
        or_scorers: List[ScriptScore] = None,
        limit: Union[Limit, int] = None,
    ) -> QueryConditionWithScoring:
        return QueryConditionWithScoring(
            condition,
            and_scorers=and_scorers,
            or_scorers=or_scorers,
            limit=Limit(limit) if isinstance(limit, int) else limit,
        )
