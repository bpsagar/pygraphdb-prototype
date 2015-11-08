import json
from urllib.request import urlopen
from bs4 import BeautifulSoup
import socket
import time

timeout = 10
socket.setdefaulttimeout(timeout)
fd = open('action_titles.txt', 'r')
data = '   '
full_data = ''
while data != '':
    data = fd.read()
    full_data += data

titles = json.loads(full_data)
count = 1
actor_links = set()

def get_actor_links_from_movie(link):
    global actor_links
    global count
    print(count)
    imdb = 'http://www.imdb.com'
    while True:
        try:
            response = urlopen(link)
            html = response.read()
        except:
            time.sleep(30)
            continue
        break
        
    soup = BeautifulSoup(str(html))
    castlist = soup.find("table", {"class":"cast_list"})
    links = castlist.findAll("td", {"class":"itemprop"})
    for l in links:
        a = l.find("a")
        ref = a['href'].split("/")[-2]
        actor_links.add(ref)
    count += 1

def generate(titles):
    for title in titles:
        link = "http://www.imdb.com/title/%s/" % title[:9]
        get_actor_links_from_movie(link)

generate(titles)

for t in threads:
    t.join()

fd = open('actors.txt', 'w')
actor_links = list(actor_links)
fd.write(json.dumps(actor_links))
fd.close()
    
