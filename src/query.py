from pygraphdb.services.client.clientapi import ClientAPI

import json
import sys

query = sys.argv[1]
query_client = ClientAPI()
query_client.connect()
msg = query_client.execute(query)
message = json.loads(msg)
print(message)