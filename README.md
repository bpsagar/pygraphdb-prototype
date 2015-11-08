# pygraphdb-prototype
A Distributed Graph Database

Getting Started
1.  Start a master process:
    $ python master.py config/master.ini
    Edit master.ini to configure the ports for communication

2.  Start a worker process:
    $ python worker.py config/worker.ini
    Edit worker.ini to configure the database location

3.  Executing Queries:
    1.  Command line:
        $ python manager.py {query}

    2.  Using the library:
        from pygraphdb.services.client.clientapi import ClientAPI
        client = ClientAPI()
        client.connect()
        msg = client.execute(query)


Example Queries:
1.  INSERT NODE Actor { Name: 'Leonardo Dicaprio', Height: '6.0' }
2.  INSERT NODE Actor { Name: 'Tom Hardy', Height: '6.1' }
3.  FIND actor:Actor where actor.Height > '5.0' return actor.Name
4.  INSERT NODE Movie { Title: 'Inception', RunTime: '90min' }
5.  INSERT EDGE ActsIn actor:Actor, movie:Movie where movie.Title == 'Inception' && actor.Name == 'Leonardo Dicaprio'
6.  INSERT EDGE ActsIn actor:Actor, movie:Movie where movie.Title == 'Inception' && actor.Name == 'Tom Hardy'
7.  FIND actor:Actor -:ActsIn> movie:Movie where movie.Title == 'Inception' return movie.Title, actor.Name
