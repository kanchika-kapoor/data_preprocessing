import requests
import pandas as pd
import json
from io import StringIO

# specify headers
header = {
    'content':'application/json', 
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:107.0) Gecko/20100101 Firefox/107.0'
}

# specify url
url = 'https://ycharts.com/events/events_data/json?date=12/01/2022&startDate=&endDate=&pageNum=5&eventGroups=earnings,dividends,splits_spinoffs,other&securitylistName=all_stocks&securityGroup=company&sort=asc'
# call get request
k = requests.get(url, headers = header)
# get relevant data from response json
data = k.json()['events']
# create dataframe from json
# newer version of pandas expects buffer of
# stringified json for read_json
data_bufr = StringIO(json.dumps(data))
df = pd.read_json(data_bufr)

print(df)
