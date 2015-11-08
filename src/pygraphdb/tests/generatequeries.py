import json
from urllib import request

fd = open('action_titles.txt', 'r')
data = '   '
full_data = ''
while data != '':
    data = fd.read()
    full_data += data

titles = json.loads(full_data)
count = 1
query_list = []

def generate_queries(titles):
    global query_list
    global count
    for title in titles:
        link = "http://www.myapifilms.com/search?idIMDB=%s&format=JSON" % (title[:9])
        try:
            response = request.urlopen(link)
        except:
            print(link)
            print(title)
            break
        
        data = response.read().decode()
        data = json.loads(data)

        str_props = [ 'title', 'rating', 'idIMDB']
        arr_props = [ 'runtime', 'genres', 'languages']
        int_props = [ 'year', 'metascore']

        prop_list = []
        for p in str_props:
            if p in data.keys():
                string = p + ":" + "'" + data[p] + "'"
                prop_list.append(string)


        for p in int_props:
            if p in data.keys():
                string = p + ":" + data[p]
                prop_list.append(string)

        for p in arr_props:
            if p in data.keys():
                val = ", ".join(data[p])
                string = p + ":" + "'" + val + "'"
                prop_list.append(string)

        name_value = ", ".join(prop_list)

        query = "INSERT NODE Movie { %s }" % name_value

        query_list.append(query)
        print(count, end='\r')
        count += 1


from threading import Thread
threads = []
for i in range(40):
    start = int((len(titles)/40) * i)
    if i == 39:
        end = len(titles)
    else:
        end = int((len(titles)/40)*(i+1))
    t = titles[start:end]
    thread = Thread(target=generate_queries, args=(t, ))
    threads.append(thread)
    thread.start()

for t in threads:
    t.join()

fd = open('insert_movies.txt', 'w')
for q in query_list:
    fd.write(q)
    fd.write('\n')
fd.close()
    
