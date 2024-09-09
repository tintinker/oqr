import json
from typing import Any, Dict


class InvalidOpenSearchOperation(Exception):
    """Custom exception for invalid OpenSearch operations."""

    def __init__(self, message: str, operation: Dict[str, Any] = None):
        self.operation = operation
        super().__init__(
            f"{message}: {json.dumps(operation, indent=2)}" if operation else message
        )
