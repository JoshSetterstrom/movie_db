import time, requests, xmltodict, itertools, string, random, json
from datetime import datetime
from pymongo import MongoClient
from bs4 import BeautifulSoup

config = json.load(open('config.json', 'r'))
start_time = datetime.now()

class movie_db():
    def __init__(self):
        self.col = MongoClient(config['db'])[config['col']][config['col_name']]


    def uptime(self, start_time):
        uptime = str(datetime.now() - start_time).split(':')
        uptime_day = (datetime.now() - start_time).days

        return (
            f'Uptime: '
            f'{uptime_day} Days '
            f'{uptime[0]} Hours '
            f'{int(uptime[1])} Minutes '
            f'{(uptime[2])[:2]} Seconds')


    def get_movie_list(self, xml):
        request = xmltodict.parse(requests.get(xml['loc']).text)

        if not request: return Exception(f'Site returned {request.status_code}: {request.reason}')

        return list(filter(lambda url: 'movie/' in url, 
                    map(lambda url: url['loc'],
                    request['urlset']['url'])))


    def get_movie_item(self, url):
        try:
            if self.col.find_one({'link': url}): return
            else:
                request = requests.get(url)
                soup = BeautifulSoup(request.text, features="lxml")
                div  = soup.find_all("div", {"class":"meta"})
                name = soup.find("div", {"class":"info"}).text.split("  ")[0][1:]
                date = div[1].text.split("Release: ")[1].split(" Director")[0]
                imdb = self.get_imdb_link(name, date)

                self.col.insert_one({
                    "link": url,
                    "id": ''.join(random.choices(string.ascii_lowercase + string.digits, k=10)),
                    "name": soup.find("div", {"class":"info"}).text.split("  ")[0][1:],
                    "searchname": '-'.join(url.split("movie/")[1].split("-")[:-1]),
                    "country": div[1].text.split("Country:  ")[1].split("  Genre")[0],
                    "genres": div[1].text.split("Genre: ")[1].split("Release")[0].replace(' ', '').split(","),
                    "year": div[1].text.split("Release: ")[1].split(" Director")[0],
                    "director": div[1].text.split("Director: ")[1].split("  Production")[0],
                    "cast": div[1].text.split("Cast:  ")[1].split("  Tags")[0].split(", "),
                    "poster": str(soup.find("img", {"class":"poster"})).split('src="')[-1:][0].split('"')[0],
                    "imdb": imdb if imdb else ""})

                print(f'Added {name} to DB.')
        
        except Exception as e: print(f'Caught Exception in get_imdb_link: {e}')
  

    def get_imdb_link(self, movie, year):
        try:
            query = movie.replace(' ', '+')
            request  = requests.get(f"https://www.google.com/search?q={query}+{year}+imdb")

            if not request: return Exception(f'Site returned {request.status_code}: {request.reason}')

            soup = BeautifulSoup(request.text, features="lxml")
            divs = soup.find_all('a')

            for div in divs:
                if not "imdb.com/title" in div.get('href'): pass
                else: return ("https://" + div.get('href').split('https://')[1].split("&sa")[0])[:-1]

            time.sleep(random.randint(2, 6)) ##To prevent too many searches at once and flag bot detection.

        except Exception as e: 
            print(f'Caught Exception in get_imdb_link: {e}')
            time.sleep(random.randint(2, 6)) ##To prevent too many searches at once and flag bot detection.
            return "" 


    def update_movie_db(self):
        while True:
            print(f"\n\n---------------{time.strftime('%Y-%m-%d %H:%M')}---------------")
            print(self.uptime(start_time))

            try:
                xml_list   = xmltodict.parse(requests.get(f"https://www.bflix.to/sitemap.xml").text)
                movie_list = list(itertools.chain.from_iterable(
                            map(self.get_movie_list, (xml_list['sitemapindex']['sitemap'])[4:])))

                for url in movie_list: self.get_movie_item(url)

                time.sleep(300)

            except: 
                print("XML parse Exception. Site is probably down.")
                time.sleep(300)


movie_db().update_movie_db()