import scrapy


class HltvSpider(scrapy.Spider):
    name = 'hltv'
    allowed_domains = ['hltv.org']
    start_urls = ['https://www.hltv.org/results']
    
    NUMLINKS = 10
    count = 0

    def parse(self, response):
        links = response.css('div.results-all > div > div.result-con > a.a-reset::attr(href)').extract()
        
        for l in links:
            yield response.follow(l, callback = self.parse_match)
            self.count += 1
            if self.count == self.NUMLINKS:
                return

        next_link = response.css('a.pagination-next::attr(href)').extract_first()
        if next_link:
            yield response.follow(next_link, callback = self.parse)

    def parse_match(self, response):
        team1 = {}
        team1["name"] = response.css("div.team1-gradient > a > div::text").extract_first()
        team1["points"] = int(response.css("div.team1-gradient > div::text").extract_first())

        team2 = {}
        team2["name"] = response.css("div.team2-gradient > a > div::text").extract_first()
        team2["points"] = int(response.css("div.team2-gradient > div::text").extract_first())

        yield {'author': author, 'title': title}

