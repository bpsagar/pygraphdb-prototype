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

for title in titles:
    link = "http://www.omdbapi.com/?i=%s&t=" % (title[:9])
    try:
        response = request.urlopen(link)
    except:
        print(link)
        print(title)
        break
    
    data = response.read().decode()
    data = json.loads(data)


    str_props = [ 'Title', 'Runtime', 'Genre', 'Language', 'imdbRating', 'imdbID']
    int_props = [ 'Year', 'Metascore']

    prop_list = []
    for p in str_props:
        if data[p] == "N/A":
            continue
        string = p + ":" + '"' + data[p] + '"'
        prop_list.append(string)

    for p in int_props:
        if data[p] == "N/A":
            continue
        string = p + ":" + data[p]
        prop_list.append(string)

    name_value = ", ".join(prop_list)

    query = "INSERT NODE Movie { %s }" % name_value

    query_list.append(query)
    print(count, end='\r')
    count += 1


fd = open('insert_movies.txt', 'w')
for q in query_list:
    fd.write(q)
    fd.write('\n')
fd.close()
    
