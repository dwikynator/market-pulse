import json
from collections.abc import Sequence
from hashlib import sha256
from io import BytesIO
from typing import Any, Literal

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError
from pydantic import BaseModel


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize a Python object into deterministic canonical JSON UTF-8 bytes."""
    # Sorted keys and compact separators guarantee identical hashes for equivalent data
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    """Compute the SHA-256 hexadecimal digest for raw bytes."""
    return sha256(value).hexdigest()


def logical_records_hash(records: Sequence[BaseModel]) -> str:
    """Generate a deterministic SHA-256 hash of records excluding ingestion metadata."""
    # Exclude ingested_at so retrying the batch with identical data produces the same hash
    rows = [record.model_dump(mode="json", exclude={"ingested_at"}) for record in records]
    return sha256_bytes(canonical_json_bytes(rows))


def records_to_parquet(records: Sequence[BaseModel]) -> bytes:
    """Encode a sequence of Pydantic models into snappy-compressed Parquet bytes."""
    if not records:
        raise ValueError("cannot encode an empty record collection")

    rows = [record.model_dump(mode="python") for record in records]
    table = pa.Table.from_pylist(rows)
    output = BytesIO()
    pq.write_table(table, output, compression="snappy")
    return output.getvalue()


class S3Storage:
    """S3 storage client supporting content-hash based idempotent writes."""

    def __init__(self, bucket: str) -> None:
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            config=Config(retries={"mode": "standard", "total_max_attempts": 3})
        )

    def put_if_changed(
        self,
        *,
        key: str,
        body: bytes,
        content_type: str,
        logical_hash: str,
    ) -> Literal["written", "unchanged"]:
        """Upload body to S3 with content hash metadata only if logical hash differs."""
        try:
            # Check metadata without downloading object body
            existing = self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            # 404/NoSuchKey indicates object doesn't exist yet; re-raise other errors
            code = error.response.get("Error", {}).get("Code")
            if code not in {"404", "NoSuchKey", "NotFound"}:
                raise
        else:
            metadata = existing.get("Metadata", {})
            if metadata.get("logical-sha256") == logical_hash:
                return "unchanged"

        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
            Metadata={"logical-sha256": logical_hash},
        )
        return "written"

