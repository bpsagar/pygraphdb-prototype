from urllib import request
import re
from threading import Thread
import time

import socket

timeout = 30
socket.setdefaulttimeout(timeout)

def titleidgenerator(urls, g):
    global titles
    ttre = re.compile('[/](tt[^/]+)[/]')
    count = 0
    for url in urls:
        print(g+ " " + str(count))
        count += 1
        while True:
            try:
                ud = request.urlopen(url)
                data = ud.read().decode()
                for item in ttre.findall(data):
                    titles.add(item)
                break
            except socket.timeout:
                print("Timeout " + g)
                time.sleep(40)
            except Exception as e:
                print("Unknown error", e.__class__.__name__)
                break
    print(g + ": done")


titles = set()

urls = []
urls.append('http://www.imdb.com/search/title?genres=action&title_type=feature&sort=moviemeter,asc')
genre = ['action', 'comedy', 'drama', 'thriller', 'horror', 'crime', 'adventure', 'animation', 'biography', 'family', 'fantasy', 'history', 'mystery', 'romance', 'sci-fi', 'sport', 'war', 'western']
#genre = ['action', 'comedy', 'drama', 'thriller', 'horror', 'crime', 'adventure', 'animation', 'biography', 'fantasy', 'history', 'mystery', 'romance', 'sci-fi', 'sport', 'war']

threads = []
for g in genre:
    urls = []
    urls.append('http://www.imdb.com/search/title?genres=%s&title_type=feature&sort=moviemeter,asc' % (g))

    i = 51
    while i < 500:
        urls.append('http://www.imdb.com/search/title?at=0&genres=%s&sort=moviemeter,asc&start=%d&title_type=feature' % (g,i))
        i += 50

    threads.append(Thread(target=titleidgenerator, args=(urls, g)))
    threads[-1].start()

for thread in threads:
    thread.join()
    
print (len(titles))
ttre = re.compile('[/](tt[^/]+)[/]')
for url in urls:
    #print (url)
    break
    while True:
        try:
            ud = request.urlopen(url)
            data = ud.read().decode()
            for item in ttre.findall(data):
                titles.add(item)
            break
        except:
            pass

       
import json
fd = open('action_titles.txt', 'w')
fd.write(json.dumps(list(titles)))
fd.close()

print (len(titles))
