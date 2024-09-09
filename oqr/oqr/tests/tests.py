import urllib3

urllib3.disable_warnings()

from oqr import (
    Index,
    MongoQueryCondition,
    and_,
    or_,
    not_,
    normalized_readable_query,
    Limit,
    ScriptScore,
)

from oqr.tests.test_secret import local_opensearch_client


def setup_test_index():
    test_index = Index(local_opensearch_client, "oqr_test_index")
    test_index.delete_index(recreate=True)
    return test_index


def insert_test_data(test_index):
    test_index.insert_one(
        {
            "name": "test_document",
            "value": 4200,
            "nested": {"field": "value"},
            "array": [0, 1, 2, 3],
            "null": None,
        },
        document_id="id0",
    )
    test_index.insert_one(
        {
            "name": "test_document",
            "value": 4201,
            "nested": {"field": "value"},
            "array": [1, 2, 3],
            "null": None,
        },
        document_id="id1",
    )
    test_index.insert_many(
        [
            {"name": "test_document_1", "value": 1, "nested": {"field": "value"}},
            {"name": "test_document_2", "value": 2, "nested": {"field": "value2"}},
            {"name": "test_document_3", "value": 3, "nested": {"field": "value3"}},
        ]
    )
    test_index.update_one("id0", {"name": "test_document4", "value": 42})
    test_index.update_one("id9", {"name": "test_document9", "value": 9}, upsert=True)
    test_index.delete_one("id1")


def run_basic_tests(test_index):
    print("\n--- Basic Tests ---")

    # Test match all query
    assert (
        normalized_readable_query(test_index._get_search_body())
        == normalized_readable_query(test_index._get_search_body({}))
        == normalized_readable_query(test_index._get_search_body(None))
    )
    results = test_index.find(None)
    print(*results, sep="\n", end="\n\n")
    assert len(results) == 5

    # Test range query
    assert (
        normalized_readable_query(
            test_index._get_search_body((test_index.value > 2) & (test_index.value < 4))
        )
        == normalized_readable_query(
            test_index._get_search_body(
                and_(test_index.value > 2, test_index.value < 4)
            )
        )
        == normalized_readable_query(
            test_index._get_search_body(
                not_(or_(test_index.value <= 2, test_index.value >= 4))
            )
        )
        == normalized_readable_query(
            test_index._get_search_body({"value": {"$gt": 2, "$lt": 4}})
        )
    )
    search_term = (test_index.value > 2) & (test_index.value < 4)
    results = test_index.find(search_term)
    print(test_index._get_search_body(search_term))
    print(*results, sep="\n", end="\n\n")
    assert len(results) == 1 and results[0]["_source"]["value"] == 3

    # Test nested field query
    assert normalized_readable_query(
        test_index._get_search_body(test_index["nested.field"] == "value2")
    ) == normalized_readable_query(
        test_index._get_search_body({"nested.field": "value2"})
    )
    search_term = test_index["nested.field"] == "value2"
    results = test_index.find(search_term)
    print(test_index._get_search_body(search_term))
    print(*results, sep="\n", end="\n\n")
    assert (
        len(results) == 1
        and results[0]["_source"]["name"] == "test_document_2"
        and results[0]["_source"]["nested"]["field"] == "value2"
    )


def run_advanced_tests(test_index):
    print("\n--- Advanced Tests ---")

    # Test negation query
    assert (
        normalized_readable_query(
            test_index._get_search_body(~(test_index["nested.field"] == "value2"))
        )
        == normalized_readable_query(
            test_index._get_search_body(not_(test_index["nested.field"] == "value2"))
        )
        == normalized_readable_query(
            test_index._get_search_body(test_index["nested.field"] != "value2")
        )
        == normalized_readable_query(
            test_index._get_search_body({"nested.field": {"$ne": "value2"}})
        )
    )
    search_term = ~(test_index["nested.field"] == "value2")
    print(test_index._get_search_body(search_term))
    print(*test_index.find(search_term), sep="\n", end="\n\n")

    # Test array query (isin)
    assert normalized_readable_query(
        test_index._get_search_body(test_index["array"].isin([1, 2]))
    ) == normalized_readable_query(
        test_index._get_search_body({"array": {"$in": [1, 2]}})
    )
    search_term = test_index["array"].isin([1, 2])
    print(test_index._get_search_body(search_term))
    print(*test_index.find(search_term), sep="\n", end="\n\n")

    # Test negation of MongoQueryCondition
    assert (
        normalized_readable_query(
            test_index._get_search_body(
                ~MongoQueryCondition({"array": {"$in": [1, 2]}})
            )
        )
        == normalized_readable_query(
            test_index._get_search_body(
                not_(MongoQueryCondition({"array": {"$in": [1, 2]}}))
            )
        )
        == normalized_readable_query(
            test_index._get_search_body(not_(test_index["array"].isin([1, 2])))
        )
    )
    search_term = ~MongoQueryCondition({"array": {"$in": [1, 2]}})
    print(test_index._get_search_body(search_term))
    print(*test_index.find(search_term), sep="\n", end="\n\n")

    # Test existence query
    assert normalized_readable_query(
        test_index._get_search_body(test_index["array"].exists())
    ) == normalized_readable_query(
        test_index._get_search_body({"array": {"$exists": True}})
    )
    search_term = test_index["array"].exists()
    print(test_index._get_search_body(search_term))
    print(*test_index.find(search_term), sep="\n", end="\n\n")

    # Test negation of existence query
    assert normalized_readable_query(
        test_index._get_search_body(~test_index["array"].exists())
    ) == normalized_readable_query(
        test_index._get_search_body({"array": {"$exists": False}})
    )
    search_term = ~test_index["array"].exists()
    print(test_index._get_search_body(search_term))
    print(*test_index.find(search_term), sep="\n", end="\n\n")

    # Test complex query
    complex_query = (
        ((test_index.value > 2) & (test_index.value < 4))
        | ((test_index.value > 8) & (test_index.value < 10))
        | (test_index.name == "test_document4")
    )
    assert (
        normalized_readable_query(test_index._get_search_body(complex_query))
        == normalized_readable_query(
            test_index._get_search_body(
                {
                    "$or": [
                        {"value": {"$gt": 2, "$lt": 4}},
                        {"value": {"$gt": 8, "$lt": 10}},
                        {"name": "test_document4"},
                    ]
                }
            )
        )
        == normalized_readable_query(
            test_index._get_search_body(
                or_(
                    and_(test_index.value > 2, test_index.value < 4),
                    and_(test_index.value > 8, test_index.value < 10),
                    test_index.name == "test_document4",
                )
            )
        )
    )
    print(test_index._get_search_body(complex_query))
    results = test_index.find(complex_query)
    print(*results, sep="\n", end="\n\n")

    # Test another complex query
    another_complex_query = and_(
        or_(test_index.value > 2, test_index.value < 4),
        and_(test_index.array.exists()),
        test_index.name != "test_document4",
    )
    print(test_index._get_search_body(another_complex_query))
    print(normalized_readable_query(test_index._get_search_body(another_complex_query)))


def run_limit_tests(test_index):
    print("\n--- Limit Tests ---")

    limit_3 = Limit(3)
    results = test_index.find(limit_3)
    print(f"Results with limit 3: {len(results)}")
    assert len(results) == 3, f"Expected 3 results, got {len(results)}"

    limit_4 = Limit(4)
    combined_limit = limit_3 & limit_4
    results = test_index.find(combined_limit)
    print(f"Results with combined limit (3 & 4): {len(results)}")
    assert len(results) == 3, f"Expected 3 results, got {len(results)}"

    combined_limit = limit_3 | limit_4
    results = test_index.find(combined_limit)
    print(f"Results with combined limit (3 | 4): {len(results)}")
    assert len(results) == 4, f"Expected 4 results, got {len(results)}"


def run_score_tests(test_index):
    print("\n--- ScriptScore Tests ---")

    score_script = ScriptScore("doc['value'].value * 0.1")
    query_with_score = (test_index.value > 0) & score_script
    print(test_index._get_search_body(query_with_score))
    results = test_index.find(query_with_score)
    print(f"Results with script score: {len(results)}")
    print(f"First result score: {results[0].get('_score', 'No score')}")
    assert all(
        "_score" in result for result in results
    ), "Expected all results to have a score"

    query_with_score_and_limit = query_with_score & Limit(3)
    print(test_index._get_search_body(query_with_score_and_limit))
    results = test_index.find(query_with_score_and_limit)
    print(f"Results with script score and limit 3: {len(results)}")
    assert len(results) == 3, f"Expected 3 results, got {len(results)}"
    assert all(
        "_score" in result for result in results
    ), "Expected all results to have a score"

    score_script_2 = ScriptScore("doc['value'].value > 100 ? 2 : 1")
    query_with_multiple_scores = (test_index.value > 0) & score_script & score_script_2
    print(test_index._get_search_body(query_with_multiple_scores))
    results = test_index.find(query_with_multiple_scores)
    print(f"Results with multiple script scores (AND): {len(results)}")
    print(f"First result score: {results[0].get('_score', 'No score')}")
    assert all(
        "_score" in result for result in results
    ), "Expected all results to have a score"

    query_with_multiple_scores_or = (
        (test_index.value > 0) | score_script | score_script_2
    )
    results = test_index.find(query_with_multiple_scores_or)
    print(f"Results with multiple script scores (OR): {len(results)}")
    print(f"First result score: {results[0].get('_score', 'No score')}")
    assert all(
        "_score" in result for result in results
    ), "Expected all results to have a score"


def run_complex_query_test(test_index):
    print("\n--- Complex Query Test ---")

    complex_query = (
        ((test_index.value > 2) & (test_index.value < 4200))
        & ScriptScore("doc['value'].value * 0.1")
        & Limit(2)
    )
    results = test_index.find(complex_query)
    print(f"Results with complex query: {len(results)}")
    print(f"Query: {test_index._get_search_body(complex_query)}")
    assert len(results) == 2, f"Expected 2 results, got {len(results)}"
    assert all(
        "_score" in result for result in results
    ), "Expected all results to have a score"


def main():
    test_index = setup_test_index()
    insert_test_data(test_index)

    run_basic_tests(test_index)
    run_advanced_tests(test_index)
    run_limit_tests(test_index)
    run_score_tests(test_index)
    run_complex_query_test(test_index)

    test_index.delete_index()
    print("\nAll tests completed successfully!")


if __name__ == "__main__":
    main()
