import time, requests, xmltodict, itertools, string, random, json, urllib


from bs4       import BeautifulSoup
from datetime  import datetime
from functools import reduce
from pymongo   import MongoClient
from threading import Thread


config     = json.load(open('config.json', 'r'))
start_time = datetime.now()


class moviedb():
    def __init__(self):
        self.movie_col   = MongoClient(config['db'])[config['movie_col']][config['movie_col_name']]
        self.flagged_col = MongoClient(config['db'])[config['movie_col']][config['flagged_col_name']]


    # Returns current runtime 
    def uptime(self, start_time):
        uptime = str(datetime.now() - start_time).split(':')
        uptime_day = (datetime.now() - start_time).days

        return (
            f'Uptime: '
            f'{uptime_day} Days '
            f'{uptime[0]} Hours '
            f'{int(uptime[1])} Minutes '
            f'{int((uptime[2])[:2])} Seconds')


    # Parse main XML sitemap then return movie URL's for each sub sitemap
    def get_movie_list(self, xml):
        request = xmltodict.parse(requests.get(xml['loc']).text)

        if not request: return Exception(f'Site returned {request.status_code}: {request.reason}')

        # Filters titles that are not movies
        return list(filter(lambda url: 'movie/' in url, 
                    map(lambda url: url['loc'],
                    request['urlset']['url'])))


    # Scrapes provided URL for movie data 
    def get_movie_item(self, url):
        try:
            if self.movie_col.find_one({'bflix': url}): return
            else:
                request   = requests.get(url)
                soup      = BeautifulSoup(request.text, features="lxml")
                div       = soup.find_all("div", {"class":"meta"})
                name      = soup.find("div", {"class":"info"}).text.split("  ")[0][1:]
                date      = div[1].text.split("Release: ")[1].split(" Director")[0]
                imdb      = self.get_imdb_link(name, date[:4])
                imdb_info = self.get_movie_info(imdb)

                if not imdb_info:
                    if self.flagged_col.find_one({'bflix': url}): return

                    # Flag movies with no imdb link for manual entry
                    self.flagged_col.insert_one({
                        "bflix":      url,
                        "cast":       [],
                        "country":    div[1].text.split("Country:  ")[1].split("  Genre")[0],
                        "director":   [],
                        "genres":     [],
                        "id":         ''.join(random.choices(string.ascii_lowercase + string.digits, k=10)),
                        "imdb":       imdb if imdb else "",
                        "name":       name,
                        "plot":       "",
                        "poster":     str(soup.find("img", {"class":"poster"})).split('src="')[-1:][0].split('"')[0],
                        "rating":     "",
                        "runtime":    "",
                        "searchname": '-'.join(url.split("movie/")[1].split("-")[:-1]),
                        "writer":     [],
                        "year":       date
                    })
                    print(f'Added {name} to flagged.')

                else: 
                    self.movie_col.insert_one({
                        "bflix":      url,
                        "cast":       imdb_info.get('cast'),
                        "country":    div[1].text.split("Country:  ")[1].split("  Genre")[0],
                        "director":   imdb_info.get('director'),
                        "genres":     imdb_info.get('genres'),
                        "id":         ''.join(random.choices(string.ascii_lowercase + string.digits, k=10)),
                        "imdb":       imdb if imdb else "",
                        "name":       name,
                        "plot":       imdb_info.get('plot'),
                        "poster":     str(soup.find("img", {"class":"poster"})).split('src="')[-1:][0].split('"')[0],
                        "rating":     imdb_info.get('rating'),
                        "runtime":    imdb_info.get('runtime'),
                        "searchname": '-'.join(url.split("movie/")[1].split("-")[:-1]),
                        "writer":     imdb_info.get('writer'),
                        "year":       date
                    })
                    print(f'Added {name} to DB.')
        
        except Exception as e: 
            print(f'Caught Exception in get_movie_item: {e}')
  

    # Scrapes Google search for imdb links
    def get_imdb_link(self, movie, year):
        try:
            request = requests.get(
                f"https://www.google.com/search?q={urllib.parse.quote_plus(movie)}+{year}+imdb")

            if not request: 
                return Exception(f'Google search returned {request.status_code}: {request.reason}')

            soup = BeautifulSoup(request.text, features="lxml")
            divs = soup.find_all('a')

            for div in divs:
                if not "imdb.com/title" in div.get('href'): continue
                else: 
                    link = ("https://" + div.get('href').split('https://')[1].split("&sa")[0])[:-1]
                    return 'https://www.imdb.com/title/tt' + link.split('/tt')[1].split('/')[0]

        except Exception as e: 
            print(f'Caught Exception in get_imdb_link: {e}')
            time.sleep(random.randint(2, 6)) ##To prevent bot detection.
            return "" 


    # Splits combined names from list 
    def get_names(self, result, value, name):
        try:
            if len(result) > 0: return result
            if not value.text == name and not value.text == f'{name}s': return result

            return list(map(lambda x: x.text, value.parent.find_all('div')[0].find('ul')))
        except:
            print(f'Unable to retrieve names for {name}.')
            return []


    # Scrapes IMDb for relevan movie info
    def get_movie_info(self, imdb):
        try: 
            if not imdb: return {}
            request = requests.get(imdb)

            if not request: 
                print(f'IMDb returned {request.status_code}: {request.reason}')
                return {}

            soup = BeautifulSoup(request.text, features="lxml")

            try: rating = soup.find('a', {'aria-label': 'View User Ratings'}).find('span').text
            except: rating = ""

            try: runtime = soup.find().text.split('Runtime')[1].split('Color')[0].split('minutes')[0] + 'minutes'
            except: runtime = ""

            return {
                'cast':     reduce(lambda x, y: self.get_names(x, y, 'Star'), soup.find_all('a'), []),
                'director': reduce(lambda x, y: self.get_names(x, y, 'Director'), soup.find_all('span'), []),
                'genres':   reduce(lambda x, y: self.get_names(x, y, 'Genre'), soup.find_all('span'), []),
                'plot':     soup.find('span', {'data-testid': 'plot-xl'}).text,
                'rating':   rating,
                'runtime':  runtime,
                'writer':   reduce(lambda x, y: self.get_names(x, y, 'Writer'), soup.find_all('span'), [])
            }
        except:
            print(f'Unable to retrieve imdb data for {imdb}')
            return {}


    # Updates IMDb ratings for movies in current and last year
    def update_rating(self):
        while True:
            movies = list(filter(lambda x: 
                    int(x['year'][:4]) 
                >= int(str(datetime.now())[:4])-1, self.movie_col.find({})))

            for movie in movies:
                if not movie['imdb']: continue
                
                request = requests.get(movie['imdb'])

                if not request: 
                    print(f'IMDb returned {request.status_code}: {request.reason}')
                    time.sleep(random.randint(2, 5))
                    continue

                soup = BeautifulSoup(request.text, features="lxml")

                try: 
                    rating = soup.find('a', {'aria-label': 'View User Ratings'}).find('span').text

                    self.movie_col.find_one_and_update(
                        {'id': movie['id']},
                        {'$set': {'rating': rating}})

                    print(f'{movie["name"]} rating has been updated.')
                except: 
                    time.sleep(random.randint(2, 5))
                    continue

            time.sleep(86400)


    # Retrieves all URL's from sitemap then updates movie_data not present in movie_db
    def update_movie_db(self):
        while True:
            print(f"\n\n---------------{time.strftime('%Y-%m-%d %H:%M')}---------------")
            print(self.uptime(start_time))

            try:
                xml_list   = xmltodict.parse(requests.get(f"https://www.bflix.ru/sitemap.xml").text)
                movie_list = list(itertools.chain.from_iterable(
                                map(self.get_movie_list, (xml_list['sitemapindex']['sitemap'])[4:])))

                print(f'{len(movie_list)} total movies in xml.')

                for url in movie_list: self.get_movie_item(url)

                print('Scrape complete.')

                time.sleep(300)

            except: 
                print("XML parse Exception. Site is probably down.")
                time.sleep(300)


Thread(target=moviedb().update_movie_db).start()
Thread(target=moviedb().update_rating).start()