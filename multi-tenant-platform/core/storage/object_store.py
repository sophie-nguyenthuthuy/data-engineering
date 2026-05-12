"""
Tenant-isolated object storage.

Each tenant writes into: s3://{bucket_prefix}-{tenant_id}/{path}
This provides hard namespace isolation — a tenant's credentials or presigned
URLs can never address another tenant's prefix even with path traversal bugs.
"""
from dataclasses import dataclass
from uuid import UUID

import aiobotocore.session

from core.config import settings


@dataclass
class StorageObject:
    key: str
    size: int
    content_type: str
    etag: str


class ObjectStore:
    def __init__(self) -> None:
        self._session = aiobotocore.session.get_session()

    def _bucket(self, tenant_id: UUID) -> str:
        return f"{settings.storage_bucket_prefix}-{tenant_id}"

    def _client(self):  # type: ignore[return]
        return self._session.create_client(
            "s3",
            endpoint_url=settings.storage_endpoint,
            aws_access_key_id=settings.storage_access_key,
            aws_secret_access_key=settings.storage_secret_key.get_secret_value(),
            region_name=settings.storage_region,
        )

    async def ensure_bucket(self, tenant_id: UUID) -> None:
        """Create the tenant's bucket if it doesn't exist (idempotent)."""
        async with self._client() as s3:
            try:
                await s3.head_bucket(Bucket=self._bucket(tenant_id))
            except Exception:
                await s3.create_bucket(Bucket=self._bucket(tenant_id))

    async def put_object(
        self,
        tenant_id: UUID,
        key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> StorageObject:
        async with self._client() as s3:
            resp = await s3.put_object(
                Bucket=self._bucket(tenant_id),
                Key=key,
                Body=data,
                ContentType=content_type,
            )
        return StorageObject(
            key=key,
            size=len(data),
            content_type=content_type,
            etag=resp["ETag"],
        )

    async def get_object(self, tenant_id: UUID, key: str) -> bytes:
        async with self._client() as s3:
            resp = await s3.get_object(Bucket=self._bucket(tenant_id), Key=key)
            async with resp["Body"] as stream:
                return await stream.read()

    async def delete_object(self, tenant_id: UUID, key: str) -> None:
        async with self._client() as s3:
            await s3.delete_object(Bucket=self._bucket(tenant_id), Key=key)

    async def list_objects(self, tenant_id: UUID, prefix: str = "") -> list[StorageObject]:
        results: list[StorageObject] = []
        async with self._client() as s3:
            paginator = s3.get_paginator("list_objects_v2")
            async for page in paginator.paginate(
                Bucket=self._bucket(tenant_id), Prefix=prefix
            ):
                for obj in page.get("Contents", []):
                    results.append(
                        StorageObject(
                            key=obj["Key"],
                            size=obj["Size"],
                            content_type="",
                            etag=obj["ETag"],
                        )
                    )
        return results

    async def presigned_get_url(
        self, tenant_id: UUID, key: str, expires_in: int = 3600
    ) -> str:
        """Generate a time-limited presigned URL for direct download."""
        async with self._client() as s3:
            return await s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": self._bucket(tenant_id), "Key": key},
                ExpiresIn=expires_in,
            )
