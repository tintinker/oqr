from typing import Union, List, Any, Dict
from opensearchpy import OpenSearch
from .query import Limit, QueryCondition, QueryConditionWithScoring, ScriptScore
from .mongostyle import MongoQueryCondition, MongoStyleOperations


class Index:
    def __init__(self, client: OpenSearch, index_name: str, index_mapping=None) -> None:
        """
        Initialize the QueryableIndex with an OpenSearch client and index name.
        :param client: The OpenSearch client
        :param index_name: The name of the index to validate fields
        :param index_mapping: The index mapping to create the index with. Optional, this option requires that the index does not already exist, so one can be created with the provided mapping.
        """
        self.client = client
        self.index_name = index_name

        self.create_if_not_exists(index_mapping=index_mapping)

        self.mongo_operations = MongoStyleOperations(
            index_name=self.index_name, client=self.client
        )

    def get_name(self) -> str:
        """
        Get the name of the index.
        """
        return self.index_name

    def get_mapping(self) -> Dict[str, Any]:
        """
        Get the mapping of the index.
        """
        return self.client.indices.get_mapping(index=self.index_name)

    def __getattr__(self, field: str) -> QueryCondition:
        """Handle field accesses via dot notation, validate field."""
        return QueryCondition(field=field)

    def __getitem__(self, field: str) -> QueryCondition:
        """Handle field accesses via bracket notation, validate field."""
        return QueryCondition(field=field)

    def match_all(self) -> QueryCondition:
        """Return a match_all query condition."""
        return QueryCondition(operator="match_all")

    def create_if_not_exists(self, index_mapping=None) -> None:
        """
        Create the index if it does not exist.
        """
        if self.client.indices.exists(index=self.index_name) and index_mapping is None:
            return
        if self.client.indices.exists(index=self.index_name):
            raise ValueError(
                f"Index '{self.index_name}' already exists, but index mapping is provided. Delete the index first to recreate or set index_mapping to None."
            )
        if index_mapping is None:
            self.client.indices.create(index=self.index_name)
        else:
            self.client.indices.create(index=self.index_name, body=index_mapping)

    def delete_index(self, recreate=False) -> None:
        """
        Delete the index.
        """
        self.client.indices.delete(index=self.index_name)

        if recreate:
            self.create_if_not_exists()

    def _get_search_body(
        self,
        query: Union[
            Dict[str, Any],
            QueryCondition,
            MongoQueryCondition,
            QueryConditionWithScoring,
            Limit,
            ScriptScore,
            None,
        ] = None,
    ) -> Dict[str, Any]:
        """
        Get the search body for the OpenSearch search API.
        """
        if query is None or isinstance(query, dict):
            query = QueryConditionWithScoring(MongoQueryCondition(query))

        elif isinstance(query, Limit):
            query = QueryConditionWithScoring(MongoQueryCondition({}), limit=query)

        elif isinstance(query, ScriptScore):
            query = QueryConditionWithScoring(
                MongoQueryCondition({}), script_score=query
            )

        elif isinstance(query, QueryCondition) or isinstance(
            query, MongoQueryCondition
        ):
            query = QueryConditionWithScoring(query)

        return query.to_dict()

    def find(
        self,
        query: Union[
            Dict[str, Any],
            QueryCondition,
            MongoQueryCondition,
            QueryConditionWithScoring,
            Limit,
            ScriptScore,
            None,
        ] = None,
        only_return_hits: bool = True,
    ) -> Dict[str, Any]:
        """
        Execute a find operation using OpenSearch's search API.
        """
        result = self.client.search(
            index=self.index_name, body=self._get_search_body(query)
        )

        if only_return_hits:
            return result["hits"]["hits"]
        return result

    def insert_one(
        self, document: Dict[str, Any], document_id: Union[str, None] = None
    ) -> None:
        """
        Add an insert (index) operation using OpenSearch's index API.
        """
        self.mongo_operations.insert_one(document=document, document_id=document_id)

    def insert_many(self, documents: List[Dict[str, Any]]) -> None:
        """
        Add bulk insert operations using OpenSearch's bulk API.
        """
        self.mongo_operations.insert_many(documents=documents)

    def update_one(
        self, document_id: str, update: Dict[str, Any], upsert: bool = False
    ) -> None:
        """
        Add update operations by converting MongoDB filter and update into OpenSearch format.
        """
        self.mongo_operations.update_one(
            document_id=document_id, update=update, upsert=upsert
        )

    def delete_one(self, document_id: str) -> None:
        """
        Add delete operations by converting MongoDB filter into an OpenSearch query.
        """
        self.mongo_operations.delete_one(document_id=document_id)
