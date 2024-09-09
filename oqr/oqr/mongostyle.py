from typing import Union, List, Dict, Any
import warnings
from typing import List, Dict, Any, Union
from .query import Query, QueryCondition
from .exceptions import InvalidOpenSearchOperation

from opensearchpy import OpenSearch


class MongoQueryCondition(QueryCondition):
    def __init__(self, mongo_query: Dict = None):
        """
        Initialize the MongoQuery by immediately converting the provided MongoDB query
        into a QueryCondition representation. If no query is provided or an empty one is
        passed, default to a match_all condition.
        """
        # Default to match_all equivalent using QueryCondition
        if not mongo_query or mongo_query == {}:
            super().__init__(field=None, value=None, operator="match_all")
        else:
            # Convert the MongoDB query to QueryCondition immediately upon initialization
            query_condition = self._parse_mongo_query(mongo_query)
            super().__init__(
                field=query_condition.field,
                value=query_condition.value,
                operator=query_condition.operator,
            )

    def _parse_mongo_query(self, mongo_query: Dict) -> QueryCondition:
        """
        Recursively parse MongoDB queries and convert them to QueryCondition objects.
        Handles mixed use of $or, $and, and implicit AND conditions.
        """
        conditions = []

        # Extract and handle $or and $and explicitly
        if "$or" in mongo_query:
            or_conditions = self._handle_logical_or(mongo_query.pop("$or"))
            conditions.append(Query.or_(*or_conditions))

        if "$and" in mongo_query:
            and_conditions = self._handle_logical_and(mongo_query.pop("$and"))
            conditions.append(Query.and_(*and_conditions))

        # Handle remaining fields as implicit AND logic
        for field, value in mongo_query.items():
            if isinstance(value, dict):
                # Handle specific operators like $gt, $in, $exists, etc.
                conditions.append(self._handle_field_conditions(field, value))
            else:
                # Handle equality by default
                conditions.append(
                    QueryCondition(field=field, value=value, operator="term")
                )

        # Combine all conditions with AND logic if multiple exist, or return a single condition
        if len(conditions) == 1:
            return conditions[0]
        return Query.and_(*conditions)

    def _handle_logical_and(self, conditions: List[Dict]) -> List[QueryCondition]:
        """
        Handle $and queries and convert them to QueryCondition 'must'.
        """
        return [self._parse_mongo_query(condition) for condition in conditions]

    def _handle_logical_or(self, conditions: List[Dict]) -> List[QueryCondition]:
        """
        Handle $or queries and convert them to QueryCondition 'should'.
        """
        return [self._parse_mongo_query(condition) for condition in conditions]

    def _handle_field_conditions(self, field: str, conditions: Dict) -> QueryCondition:
        """
        Convert MongoDB-style field conditionals like $gt, $lt, $in, etc., to QueryCondition objects.
        """
        if (
            "$gt" in conditions
            or "$lt" in conditions
            or "$gte" in conditions
            or "$lte" in conditions
        ):
            return self._handle_range_query(field, conditions)
        elif "$in" in conditions:
            return QueryCondition(
                field=field, value=conditions["$in"], operator="terms"
            )
        elif "$ne" in conditions:
            return Query.not_(
                QueryCondition(field=field, value=conditions["$ne"], operator="term")
            )
        elif "$exists" in conditions:
            if conditions["$exists"]:
                return QueryCondition(field=field, value=None, operator="exists")
            else:
                return Query.not_(
                    QueryCondition(field=field, value=None, operator="exists")
                )
        else:
            # Handle equality by default
            return QueryCondition(field=field, value=conditions, operator="term")

    def _handle_range_query(self, field: str, conditions: Dict) -> QueryCondition:
        """
        Convert MongoDB range conditions ($gt, $lt, $gte, $lte) to QueryCondition range queries.
        """
        range_conditions = []
        if "$gt" in conditions:
            range_conditions.append({"gt": conditions["$gt"]})
        if "$lt" in conditions:
            range_conditions.append({"lt": conditions["$lt"]})
        if "$gte" in conditions:
            range_conditions.append({"gte": conditions["$gte"]})
        if "$lte" in conditions:
            range_conditions.append({"lte": conditions["$lte"]})

        return Query.and_(
            [
                QueryCondition(field=field, value=range_condition, operator="range")
                for range_condition in range_conditions
            ]
        )

    def __repr__(self):
        """
        Return the string representation of the QueryCondition when the MongoQuery object is represented.
        """
        return repr(self.to_dict())


class MongoStyleOperations:
    def __init__(self, client: OpenSearch, index_name: str):
        """
        Initialize MongoStyleOperations with an OpenSearch client and the required index name.
        """
        self.client = client
        self.index_name = index_name

    def _convert_filter(self, mongo_filter: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert MongoDB-style filters into OpenSearch queries.
        """
        try:
            query = MongoQueryCondition(mongo_filter)
            return query.to_dict()
        except Exception as e:
            raise InvalidOpenSearchOperation(
                f"Invalid filter in query", mongo_filter
            ) from e

    def insert_one(
        self, document: Dict[str, Any], document_id: Union[str, None] = None
    ) -> None:
        """
        Insert a single document using OpenSearch's index API.
        """
        if not isinstance(document, dict) or not document:
            raise InvalidOpenSearchOperation(
                "Document for insert_one must be a non-empty dictionary", document
            )

        try:
            # Use OpenSearch client's index method
            response = self.client.index(
                index=self.index_name, id=document_id, body=document, refresh=True
            )
            if response.get("result") not in ["created", "updated"]:
                raise InvalidOpenSearchOperation(f"Failed to insert document", document)
        except Exception as e:
            raise InvalidOpenSearchOperation(
                f"Failed to execute insert_one", document
            ) from e

    def insert_many(self, documents: List[Dict[str, Any]]) -> None:
        """
        Insert multiple documents using OpenSearch's bulk API.
        """
        if not isinstance(documents, list) or not all(
            isinstance(doc, dict) for doc in documents
        ):
            raise InvalidOpenSearchOperation(
                "Documents for insert_many must be a list of dictionaries", documents
            )

        # Prepare bulk operations
        bulk_operations = []
        for doc in documents:
            bulk_operations.append({"index": {"_index": self.index_name}})
            bulk_operations.append(doc)

        # Execute bulk operation
        try:
            response = self.client.bulk(body=bulk_operations, refresh=True)
            if response["errors"]:
                raise InvalidOpenSearchOperation(
                    f"Failed to insert documents via bulk", response
                )
        except Exception as e:
            raise InvalidOpenSearchOperation(
                f"Failed to execute insert_many", documents
            ) from e

    def update_one(
        self, document_id: str, update: Dict[str, Any], upsert: bool = False
    ):
        """
        Update a single document by converting MongoDB filter and update into OpenSearch format.
        """
        if not update:
            raise InvalidOpenSearchOperation(
                "Update document must be provided for update_one", update
            )

        try:
            # Use OpenSearch's update API to apply the update
            response = self.client.update(
                index=self.index_name,
                id=document_id,
                body={"doc": update, "doc_as_upsert": upsert},
                refresh=True,
            )

            # Warn if no documents were updated, but do not raise an exception
            if response.get("result") not in ["updated", "created"]:
                warnings.warn(
                    f"No documents were updated for the given filter. response: {response}",
                    stacklevel=3,
                )

        except Exception as e:
            raise InvalidOpenSearchOperation(
                f"Failed to execute update_one",
                {"document_id": document_id, "update": update},
            ) from e

    def delete_one(self, document_id: str) -> None:
        """
        Delete a single document by ID using OpenSearch's delete API.
        """
        try:
            response = self.client.delete(
                index=self.index_name, id=document_id, refresh=True
            )
            if response.get("result") != "deleted":
                raise InvalidOpenSearchOperation(
                    f"Failed to delete document with ID {document_id}", response
                )
        except Exception as e:
            raise InvalidOpenSearchOperation(
                f"Failed to execute delete_one", document_id
            ) from e
