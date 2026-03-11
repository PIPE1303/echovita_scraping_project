import scrapy


class ObituaryItem(scrapy.Item):
    full_name = scrapy.Field()
    date_of_birth = scrapy.Field()
    date_of_death = scrapy.Field()
    obituary_text = scrapy.Field()
    city = scrapy.Field()
    state = scrapy.Field()
    url = scrapy.Field()
