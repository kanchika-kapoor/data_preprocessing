import requests
import pandas as pd
import json
from io import StringIO

# specify headers
header = {'content':'application/json'}
# specify url
url = 'https://killedbygoogle.com/_next/data/kwu8voS3XHNDh6ya51_rK/index.json'
# call get request
k = requests.get(url, headers = header)
# get relevant data from response json
data = k.json()['pageProps']['items']
# create dataframe from json
# newer version of pandas expects buffer of
# stringified json for read_json
data_bufr = StringIO(json.dumps(data))
df = pd.read_json(data_bufr)

print(df)