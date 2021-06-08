import scrapy
from random import random
from datetime import datetime
from dateutil.relativedelta import relativedelta


class HltvSpider(scrapy.Spider):
    name = 'hltv'

    allowed_domains = ['hltv.org']
    start_urls = [f"https://www.hltv.org/results?offset={off * 100}" for off in range(100)]
    
    matches = 0
    events = 0

    def parse(self, response):
        links = response.css('div.results-all > div > div.result-con > a.a-reset::attr(href)').extract()
        
        print("\n\n***\n", len(links), "\n***\n\n")
        for l in links:
            yield response.follow(l, callback = self.parse_match)

    def parse_match(self, response):
        team1 = {}
        team1["name"] = response.css("div.team1-gradient > a > div::text").extract_first()
        team1["points"] = int(response.css("div.team1-gradient > div::text").extract_first())

        team2 = {}
        team2["name"] = response.css("div.team2-gradient > a > div::text").extract_first()
        team2["points"] = int(response.css("div.team2-gradient > div::text").extract_first())

        details = response.css("div.maps > div > div.veto-box > div.preformatted-text::text").extract_first()
        details = details.splitlines()

        best_of = details[0]
        match_type = details[2]

        match_time = int(response.css("div.timeAndEvent > div.date::attr(data-unix)").extract_first())
        match_datetime = datetime.utcfromtimestamp(match_time / 1000)
        match_date = match_datetime.strftime('%Y-%m-%d')

        player_stats_to = match_datetime.strftime('%Y-%m-%d')
        player_stats_from = (match_datetime - relativedelta(months=6)).strftime('%Y-%m-%d')

        maps = response.css("div.mapholder > div > div.map-name-holder > div.mapname::text").extract()

        lineups = response.css("div.lineups > div > div.lineup > div.players")
        lineups_urls = []
        for i in range(2):
            urls = lineups[i].css("td.player-image > a::attr(href)").extract()
            urls = list(map(lambda x: x.replace("player", "stats/players") + f"?startDate={player_stats_from}&endDate={player_stats_to}", urls))
            lineups_urls.append(urls)

        match = {'team1': team1, 
                'lineup1': lineups_urls[0],
                'team2': team2, 
                'lineup2': lineups_urls[1],
                'best_of': best_of, 
                'match_type': match_type,
                'maps': maps,
                'date': match_date,
                'unix_time': match_time }
        event_url = response.css("div.timeAndEvent > div.event > a::attr(href)").extract_first()
        
        self.matches += 1
        print(["*" for _ in range(10)], 
            f"self.matches = {self.matches}\tself.events = {self.events}")
        yield response.follow(event_url + "?_=" + str(random()), 
                callback = self.get_parse_event(match))

    def get_parse_event(self, match):
        def parse_event(response):
            event = {}
            event["prize"] = response.css("table.info > tbody > tr > td.prizepool::text").extract_first()
            event["teams"] = response.css("table.info > tbody > tr > td.teamsNumber::text").extract_first()
            event["location"] = response.css("table.info > tbody > tr > td.location > div > span::text").extract_first()
            
            match["event"] = event

            self.events += 1
            print(["*" for _ in range(10)], 
                f"self.matches = {self.matches}\tself.events = {self.events}")

            #yield match
            yield response.follow(match['lineup1'][0] + "?_=" + str(random()), 
                callback = self.get_parse_lineups(match))
        return parse_event
        
    def get_parse_lineups(self, match):
        def parse_lineups(response):
            next = None
            for i in range(1, 3):
                for j in range(5):
                    player = match['lineup' + str(i)][j]
                    if isinstance(player, str):
                        if player in response.url:
                            p = {}
                            p['name'] = response.css("div.summaryBreakdownContainer > div.summaryShortInfo > h1.summaryNickname::text").extract_first()
                            stats = response.css("div.summaryBreakdownContainer > div.summaryStatBreakdownRow > div.summaryStatBreakdown > div.summaryStatBreakdownData > div.summaryStatBreakdownDataValue::text").extract()

                            p['rating'] = float(stats[0])
                            p['DPR'] = float(stats[1])
                            p['KAST'] = float(stats[2].strip('%')) / 100
                            p['impact'] = float(stats[3])
                            p['ADR'] = float(stats[4])
                            p['KPR'] = float(stats[5])
                            match['lineup' + str(i)][j] = p
                        elif next == None:
                            next = player

            if next:
                yield response.follow(next + "?_=" + str(random()), 
                    callback = self.get_parse_lineups(match))
            else:
                yield match
        return parse_lineups
        

