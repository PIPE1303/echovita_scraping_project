import json
import logging
from pathlib import Path

import scrapy
from scrapy import Spider

logger = logging.getLogger(__name__)


def _item_to_dict(item) -> dict:
    """Serialise any scrapy.Item to a plain dict."""
    return dict(item)


def _s3_key(item) -> str:
    """Derive a storage key from the item's url field, falling back to the item class name."""
    data = dict(item)
    url = data.get("url", "")
    slug = url.strip("/").split("/")[-1] if url else type(item).__name__.lower()
    return f"obituaries/{slug}.json"


def _manifest_path(spider: Spider, filename: str) -> Path:
    root = Path(spider.settings.get("PROJECT_ROOT", ".")).resolve()
    return root / filename


class MockS3Pipeline:
    """
    Simulates S3 uploads by serialising items to an in-memory dict.
    Writes a JSON manifest on spider close for downstream validation.
    Accepts any scrapy.Item subclass — no changes needed when adding new spiders.
    """

    def open_spider(self, spider: Spider):
        spider.s3_uploaded_items: dict[str, str] = {}

    def process_item(self, item, spider: Spider):
        if not isinstance(item, scrapy.Item):
            return item
        key = _s3_key(item)
        spider.s3_uploaded_items[key] = json.dumps(_item_to_dict(item), ensure_ascii=False)
        logger.info("Mock S3 upload: %s", key)
        return item

    def close_spider(self, spider: Spider):
        uploaded = getattr(spider, "s3_uploaded_items", {})
        manifest = {"count": len(uploaded), "keys": list(uploaded.keys())}
        path = _manifest_path(spider, "upload_manifest_s3.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(manifest, f)
        logger.info("Mock S3: %d items — manifest: %s", manifest["count"], path)


class MockGCSPipeline:
    """
    Simulates GCS uploads by serialising items to an in-memory dict.
    Writes a JSON manifest on spider close for downstream validation.
    Accepts any scrapy.Item subclass — no changes needed when adding new spiders.
    """

    def open_spider(self, spider: Spider):
        spider.gcs_uploaded_items: dict[str, str] = {}

    def process_item(self, item, spider: Spider):
        if not isinstance(item, scrapy.Item):
            return item
        key = _s3_key(item)
        spider.gcs_uploaded_items[key] = json.dumps(_item_to_dict(item), ensure_ascii=False)
        logger.info("Mock GCS upload: %s", key)
        return item

    def close_spider(self, spider: Spider):
        uploaded = getattr(spider, "gcs_uploaded_items", {})
        manifest = {"count": len(uploaded), "keys": list(uploaded.keys())}
        path = _manifest_path(spider, "upload_manifest_gcs.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(manifest, f)
        logger.info("Mock GCS: %d items — manifest: %s", manifest["count"], path)


class RealS3Pipeline:
    """
    Real S3 upload via boto3. Disabled by default — not listed in ITEM_PIPELINES.

    Enable by adding to ITEM_PIPELINES at priority 150 (between mock S3 and GCS).

    Required settings (env vars preferred over hardcoding):
        S3_BUCKET_NAME, S3_KEY_PREFIX, AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
    """

    def open_spider(self, spider: Spider):
        try:
            import boto3
            from botocore.exceptions import BotoCoreError, ClientError
        except ImportError as exc:
            raise RuntimeError("boto3 is required for RealS3Pipeline.") from exc

        self._ClientError = ClientError
        self._BotoCoreError = BotoCoreError
        self.bucket = spider.settings.get("S3_BUCKET_NAME")
        if not self.bucket:
            raise ValueError("S3_BUCKET_NAME must be set to use RealS3Pipeline")

        self.prefix = spider.settings.get("S3_KEY_PREFIX", "obituaries/")
        self.client = boto3.client(
            "s3",
            aws_access_key_id=spider.settings.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=spider.settings.get("AWS_SECRET_ACCESS_KEY"),
            region_name=spider.settings.get("AWS_DEFAULT_REGION", "us-east-1"),
        )
        spider.s3_real_uploaded_keys = []
        logger.info("RealS3Pipeline: connected to bucket '%s'", self.bucket)

    def process_item(self, item, spider: Spider):
        if not isinstance(item, scrapy.Item):
            return item

        key = _s3_key(item)
        body = json.dumps(_item_to_dict(item), ensure_ascii=False)

        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json",
            )
            spider.s3_real_uploaded_keys.append(key)
            logger.info("S3 upload OK: s3://%s/%s", self.bucket, key)
        except (self._BotoCoreError, self._ClientError) as exc:
            logger.error("S3 upload failed for %s: %s", key, exc)

        return item

    def close_spider(self, spider: Spider):
        count = len(getattr(spider, "s3_real_uploaded_keys", []))
        logger.info("RealS3Pipeline: %d items uploaded to s3://%s", count, self.bucket)


class JsonlExportPipeline:
    """Accumulates items during the crawl and writes a JSONL file on close."""

    def open_spider(self, spider: Spider):
        spider.jsonl_items = []

    def process_item(self, item, spider: Spider):
        if isinstance(item, scrapy.Item):
            spider.jsonl_items.append(_item_to_dict(item))
        return item

    def close_spider(self, spider: Spider):
        path = (
            getattr(spider, "jsonl_output_path", None)
            or spider.settings.get("JSONL_OUTPUT_PATH", "obituaries.jsonl")
        )
        root = Path(spider.settings.get("PROJECT_ROOT", ".")).resolve()
        out_path = root / path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        items = getattr(spider, "jsonl_items", [])
        with open(out_path, "w", encoding="utf-8") as f:
            for obj in items:
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        logger.info("Exported %d items to %s", len(items), out_path)
