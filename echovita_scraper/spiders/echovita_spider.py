import re
from urllib.parse import urljoin

import scrapy

from echovita_scraper.items import ObituaryItem

_INLINE_CTA = re.compile(
    r"\s*(?:Family and friends are welcome to|You can send your sympathy|"
    r"Leave a sympathy message|Family and friends can send flowers|"
    r"You may also light a candle).*$",
    re.IGNORECASE | re.DOTALL,
)


class EchovitaSpider(scrapy.Spider):
    """Crawls Echovita obituary listing and detail pages."""

    name = "echovita"
    allowed_domains = ["www.echovita.com", "echovita.com"]
    start_urls = ["https://www.echovita.com/us/obituaries/"]

    MAX_PAGES = 5

    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "CONCURRENT_REQUESTS": 2,
        "DOWNLOAD_DELAY": 1.0,
    }

    def parse(self, response):
        for link in response.xpath('//a[contains(@class, "text-name-obit-in-list")]/@href').getall():
            yield scrapy.Request(
                urljoin(response.url, link),
                callback=self.parse_obituary_detail,
                errback=self.errback_detail,
            )

        page = response.meta.get("page", 1)
        if page < self.MAX_PAGES:
            next_page = page + 1
            base = response.url.split("?")[0].rstrip("/")
            yield scrapy.Request(
                f"{base}?page={next_page}",
                callback=self.parse,
                meta={"page": next_page},
            )

    def parse_obituary_detail(self, response):
        item = ObituaryItem()
        item["url"] = response.url

        name_text = response.css("div.obit-main-info-wrapper-min-height p.my-auto::text").get()
        if name_text:
            name_text = name_text.strip()
        else:
            name_text = (response.css("h1::text").get() or "").strip()
            if name_text.endswith(" Obituary"):
                name_text = name_text[:-9].strip()
        item["full_name"] = name_text or None

        date_nodes = response.xpath(
            '//p[.//i[contains(@class, "fa-calendar-day")]]/text()'
        ).getall()
        date_nodes = [t.strip() for t in date_nodes if t.strip() and re.match(r"[A-Za-z]", t.strip())]
        item["date_of_birth"] = date_nodes[0] if len(date_nodes) > 0 else None
        item["date_of_death"] = date_nodes[1] if len(date_nodes) > 1 else None

        location = response.xpath(
            '//p[.//i[contains(@class, "fa-map-marker-alt")]]/a/text()'
        ).getall()
        item["city"] = location[0].strip() if len(location) > 0 else None
        item["state"] = location[1].strip() if len(location) > 1 else None

        obituary_parts = []
        for para in response.css("div#obituary p, div.ObituaryDescText p"):
            text = " ".join(t.strip() for t in para.css("::text").getall() if t.strip())
            text = _INLINE_CTA.sub("", text).strip()
            if len(text) > 20:
                obituary_parts.append(text)

        obituary_text = " ".join(obituary_parts) if obituary_parts else None

        if obituary_text and item.get("full_name"):
            fname = item["full_name"].strip()
            while obituary_text.startswith(fname):
                obituary_text = obituary_text[len(fname):].strip()
        item["obituary_text"] = obituary_text or None

        yield item

    def errback_detail(self, failure):
        self.logger.warning("Failed to fetch detail page: %s", failure.request.url)
