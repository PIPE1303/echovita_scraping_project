import os

BOT_NAME = "echovita_scraper"
SPIDER_MODULES = ["echovita_scraper.spiders"]
NEWSPIDER_MODULE = "echovita_scraper.spiders"

ROBOTSTXT_OBEY = True

ITEM_PIPELINES = {
    "echovita_scraper.pipelines.MockS3Pipeline": 100,
    # "echovita_scraper.pipelines.RealS3Pipeline": 150,
    "echovita_scraper.pipelines.MockGCSPipeline": 200,
    "echovita_scraper.pipelines.JsonlExportPipeline": 300,
}

# include/outputs/ is mounted by Astro CLI, keeping output paths consistent
# between local runs and Airflow container execution.
PROJECT_ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "include", "outputs",
)
JSONL_OUTPUT_PATH = "obituaries.jsonl"

CONCURRENT_REQUESTS = 2
DOWNLOAD_DELAY = 1.0
DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "scrapy.downloadermiddlewares.retry.RetryMiddleware": 550,
}

LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
