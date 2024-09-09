# oqr Library

## Overview

The **OQR** (OpenSearch Query Resolver) library provides a flexible interface for querying **OpenSearch** using both **Pandas-style expressions**, **MongoDB-style queries**, and **native Python operators**. This library is designed to streamline access to OpenSearch indices, allowing users to construct complex queries effortlessly.

Key Features:

- **Pandas-Style and MongoDB Queries**: Query using Python's symbolic operators (`&`, `|`, `~`), logical functions (`and_`, `or_`, `not_`), or MongoDB-style syntax.
- **Simplified CRUD-like interfaces**: Familiar find, insert, update, and delete for MongoDB users
- **Query Validation**: Queries are validated with helpful error messages for incorrect syntax or logic.

## Setup

```python
from opensearchpy import OpenSearch
from oqr import Index, MongoQueryCondition, and_, or_, not_, normalized_readable_query

# Assume you've already set up your OpenSearch client
opensearch_client = OpenSearch(...)

# Create an Index object
test_index = Index(opensearch_client, "oqr_test_index")

# To create an index with a mapping, use the index_mapping option
test_index_with_explicit_mapping = Index(opensearch_client, "oqr_test_index_with_explicit_mapping", index_mapping = { "mappings": {"properties": {}}, "settings": "index.mapping.total_fields.limit": 10 })

# Get the name, mapping
mapping = test_index.get_name()
mapping = test_index.get_mapping()
```

Note: Using the `Index` constructor will create the index if it doesn't exist. Methods like `.find()` execute operations on the index via the stored client.
Note: Using the `index_mapping` option requires that the index not already exist. In OpenSearch, to modify the mapping, indicies must be deleted and re-created

## Index Operations

```python
# Create/recreate index
test_index.delete_index(recreate=True)
```

## Document Operations

```python
# Insert single document
test_index.insert_one(
    {
        "name": "test_document",
        "value": 4200,
        "nested": {"field": "value"},
        "array": [0, 1, 2, 3],
        "null": None,
    },
    document_id="id0"  # optional
)

# Insert multiple documents
test_index.insert_many([
    {"name": "test_document_1", "value": 1, "nested": {"field": "value"}},
    {"name": "test_document_2", "value": 2, "nested": {"field": "value2"}},
    {"name": "test_document_3", "value": 3, "nested": {"field": "value3"}},
])

# Update document
test_index.update_one("id0", {"name": "test_document4", "value": 42})
test_index.update_one("id9", {"name": "test_document9", "value": 9}, upsert=True)

# Delete document
test_index.delete_one("id1")
```

## Querying

### Basic Query (Match All)

```python
results = test_index.find()  # or test_index.find({})
```

OpenSearch JSON:

```json
{
  "query": {
    "match_all": {}
  }
}
```

Expected results:

```python
[
    {"_id": "id0", "_source": {"name": "test_document4", "value": 42, ...}},
    {"_id": "...", "_source": {"name": "test_document_1", "value": 1, ...}},
    ...
]
```

### Mixing MongoDB and Pandas syntax

To combine MongoDB syntax with Pandas syntax, wrap the MongoDB-style query in the `MongoQueryCondition` class.
For example, to negate the range query below, the following syntax is valid

```python
search_term = not_((test_index.value > 2) & (test_index.value < 4))
search_term = not_(MongoQueryCondition({"value": {"$gt": 2, "$lt": 4}}))
search_term = ~MongoQueryCondition({"value": {"$gt": 2, "$lt": 4}})
```

### Comparing and Printing Normalized Readable Queries

Sometimes it is useful to view a query in a less verbose format, or to ensure that complex queries are functionally equivalent. For this use case `oqr`, provides the `normalized_readable_query` function.  
All queries passed to this function are converted to a readable string and normalized for comparison such that

- `x != y` becomes `not(x == y)`
- `x > y` becomes `not(x <= y)`
- `x >= y` becomes `not(x < y)`
- `not(not(x))` becomes `x`
- `or(or(x,y),z)` becomes `or(x,y,z)` (as well as and)
- `not(and(x,y))` becomes `or(not(x), not(y))` (all not conditions are pushed to the innermost level using De Morgan's laws)
- `and(z,x,y)` becomes `and(x,y,z)` (sorted for comparison)

```python
print(normalized_readable_query(
        test_index._get_search_body(
            and_(
                or_(
                    test_index.value > 2,
                    test_index.value < 4,
                ),
                and_(
                    test_index.array.exists()
                ),
                test_index.name != "test_document4",
            )
        )
    ))
"and(exists(array), not(name == test_document4), or(not(value <= 2), value < 4))"
```

### Complex Query (OR condition with nested AND)

```python
# Using symbols
search_term = ((test_index.value > 2) & (test_index.value < 4)) | ((test_index.value > 8) & (test_index.value < 10))

# Using logical operator functions
search_term = or_(
    and_(test_index.value > 2, test_index.value < 4),
    and_(test_index.value > 8, test_index.value < 10)
)

# Using MongoDB syntax
search_term = {
    "$or": [
        {"value": {"$gt": 2, "$lt": 4}},
        {"value": {"$gt": 8, "$lt": 10}}
    ]
}

results = test_index.find(search_term)
```

OpenSearch JSON:

```json
{
  "query": {
    "bool": {
      "should": [
        {
          "bool": {
            "must": [
              { "range": { "value": { "gt": 2 } } },
              { "range": { "value": { "lt": 4 } } }
            ]
          }
        },
        {
          "bool": {
            "must": [
              { "range": { "value": { "gt": 8 } } },
              { "range": { "value": { "lt": 10 } } }
            ]
          }
        }
      ]
    }
  }
}
```

Expected results:

```python
[
    {"_id": "...", "_source": {"name": "test_document_3", "value": 3, ...}},
    {"_id": "id9", "_source": {"name": "test_document9", "value": 9, ...}}
]
```

## Advanced Usage

### Limits

The OQR library provides a `Limit` class to restrict the number of results returned by a query.

```python
from oqr import Limit

# Create a Limit object
limit_5 = Limit(5)

# Apply the limit to a query
results = test_index.find(limit_5)
```

You can combine Limit objects with other query conditions:

```python
query_with_limit = (test_index.value > 0) & Limit(10)
results = test_index.find(query_with_limit)
```

Limit objects can be combined using `&` (minimum) and `|` (maximum) operators:

```python
limit_5 = Limit(5)
limit_10 = Limit(10)

min_limit = limit_5 & limit_10  # Results in Limit(5)
max_limit = limit_5 | limit_10  # Results in Limit(10)
```

### Scoring

The OQR library supports custom scoring of search results using the `ScriptScore` class. This allows you to apply custom scoring logic to your queries.

```python
from oqr import ScriptScore

# Create a ScriptScore object
score_script = ScriptScore("doc['value'].value * 0.1")

# Apply the score to a query
query_with_score = (test_index.value > 0) & score_script

results = test_index.find(query_with_score)
```

Note: You can directly combine `ScriptScore` objects with `QueryCondition` objects using the `&` operator. There's no need to use `Query.with_scoring()`.

You can also combine multiple ScriptScore objects:

```python
score_script_1 = ScriptScore("doc['value'].value * 0.1")
score_script_2 = ScriptScore("doc['popularity'].value * 0.5")

# Combine scores using AND (both scores will be applied)
query_with_multiple_scores = (test_index.value > 0) & score_script_1 & score_script_2

# Combine scores using OR (the maximum score will be used)
query_with_or_scores = (test_index.value > 0) | score_script_1 | score_script_2

results = test_index.find(query_with_multiple_scores)
```

### Combining Scoring and Limits

You can create complex queries that combine conditions, scoring, and limits:

```python
complex_query = (
    ((test_index.value > 2) & (test_index.value < 4200))
    & ScriptScore("doc['popularity'].value * 0.5")
    & Limit(3)
)

results = test_index.find(complex_query)
```

This query will:

1. Filter documents where the value is between 2 and 4200
2. Apply a custom score based on the 'popularity' field
3. Limit the results to the top 3 documents

### Visualizing Queries

To see the OpenSearch query generated by OQR, you can use the `_get_search_body()` method:

```python
complex_query = (
    ((test_index.value > 2) & (test_index.value < 4200))
    & ScriptScore("doc['popularity'].value * 0.5")
    & Limit(3)
)

print(test_index._get_search_body(complex_query))
```

This will output the OpenSearch JSON query, which can be useful for debugging or understanding the generated query structure.

### Range Query

```python
# Using symbols
search_term = (test_index.value > 2) & (test_index.value < 4)

# Using logical operator functions
search_term = and_(test_index.value > 2, test_index.value < 4)

# Using MongoDB syntax
search_term = {"value": {"$gt": 2, "$lt": 4}}

results = test_index.find(search_term)
```

OpenSearch JSON:

```json
{
  "query": {
    "bool": {
      "must": [
        { "range": { "value": { "gt": 2 } } },
        { "range": { "value": { "lt": 4 } } }
      ]
    }
  }
}
```

Expected results:

```python
[
    {"_id": "...", "_source": {"name": "test_document_3", "value": 3, ...}}
]
```

### Nested Field Query

```python
# Using symbols
search_term = test_index["nested.field"] == "value2"

# Using MongoDB syntax
search_term = {"nested.field": "value2"}

results = test_index.find(search_term)
```

OpenSearch JSON:

```json
{
  "query": {
    "term": {
      "nested.field": "value2"
    }
  }
}
```

Expected results:

```python
[
    {"_id": "...", "_source": {"name": "test_document_2", "value": 2, "nested": {"field": "value2"}, ...}}
]
```

### Negation Query

```python
# Using symbols
search_term = (test_index["nested.field"] != "value2")
search_term = ~(test_index["nested.field"] == "value2")

# Using logical operator functions
search_term = not_(test_index["nested.field"] == "value2")

# Using MongoDB syntax
search_term = {"nested.field": {"$ne": "value2"}}

results = test_index.find(search_term)
```

OpenSearch JSON:

```json
{
  "query": {
    "bool": {
      "must_not": [{ "term": { "nested.field": "value2" } }]
    }
  }
}
```

### Array Query (isin)

```python
# Using symbols
search_term = test_index["array"].isin([1, 2])

# Using MongoDB syntax
search_term = {"array": {"$in": [1, 2]}}

results = test_index.find(search_term)
```

OpenSearch JSON:

```json
{
  "query": {
    "terms": {
      "array": [1, 2]
    }
  }
}
```

Expected results:

```python
[
    {"_id": "id0", "_source": {"name": "test_document4", "value": 42, "array": [0, 1, 2, 3], ...}}
]
```

### Existence Query

```python
# Using symbols
search_term = test_index["array"].exists()

# Using MongoDB syntax
search_term = {"array": {"$exists": True}}

results = test_index.find(search_term)
```

OpenSearch JSON:

```json
{
  "query": {
    "exists": {
      "field": "array"
    }
  }
}
```

Expected results:

```python
[
    {"_id": "id0", "_source": {"name": "test_document4", "value": 42, "array": [0, 1, 2, 3], ...}}
]
```
