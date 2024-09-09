from opensearchpy import OpenSearch

local_opensearch_client = OpenSearch(
    hosts=[{"host": "opensearch-node", "port": 9200}],
    http_auth=("admin", "localPASS1234!@#"),
    use_ssl=True,
    verify_certs=False,
)
