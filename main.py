import requests
from io import StringIO
import pandas as pd
import json
import pypyodbc
from urllib.parse import urlencode


def dict_to_query_params(query_params):
  """
  method for dictionary to query param convertion
  returns url encoded string
  """
  return urlencode(query_params)

def get_json_response(url, headers):
  """
  calls the url with given headers using get
  returns json response
  """
  try:
    resp = requests.get(url, headers=headers)
    data = resp.json()
    return data
  except Exception as e:
    print('error: '+str(e))

def clean_fetched_data(json_data):
    """
    clean the data received from api
    returns dataframe
    """
    json_str = json.dumps(json_data) #convert dictionary to string
    json_buf = StringIO(json_str) # make string buffer

    df = pd.read_json(json_buf)
    df = df.drop(['event_date_fmtd', 'event_date_js', 'event_datetime_utc_js'], axis=1)
    df = df.drop(['event_time_fmtd'], axis=1)
    df.currency_code = df.currency_code.fillna('Unknown')
    df = df.drop(['description'], axis=1)
    return df

def get_paginated_data_arr(base_url,headers,params,data_key_name,total_pages_num,curr_page_num_key):
  """
  loops till total_pages_num and fetches data for every page
  returns a list containing all data
  """
  try:
    total_arr = []
    for i in range(1, total_pages_num+1):
      url = base_url + dict_to_query_params(params)
      data = get_json_response(url, headers)[data_key_name]
      total_arr += data 
      params[curr_page_num_key]+=1
    return total_arr
  except Exception as e:
    print('error: '+str(e))

def get_db_connection(host, port, username, password, database):
  connection = pypyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
    'Server='+host+','+port+';'
    'Database='+database+';'
    'encrypt=yes;'
    'TrustServerCertificate=yes;'
    'UID='+username+';'
    'PWD='+password+';',autocommit = True)
  return connection

def get_chunked_dataframe(max_row_count, dataframe):
  """
  split dataframe into n parts based on max_row_count
  returns list of dataframes
  """
  arr=[]
  chunk_size = round(len(dataframe)/max_row_count)
  for i in range(chunk_size):
    arr.append(dataframe[i*max_row_count:max_row_count*(i+1)])
  return arr


if __name__ == '__main__':
    header = {'Accept': 'application/json',
          'Content':'application/json',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:107.0) Gecko/20100101 Firefox/107.0'
    }
    start_date = '11/20/2022'
    end_date = '12/01/2022'
    page_num = 1

    params = {
        'startDate': start_date,
        'endDate':end_date,
        'pageNum':page_num,
        'eventGroups':'earnings,dividends,splits_spinoffs,other',
        'securitylistName':'all_stocks',
        'securityGroup':'company',
        'sort':'asc'
    }

    base_url = 'https://ycharts.com/events/events_data/json'

    url = base_url+'?'+dict_to_query_params(params)
    data = get_json_response(url, header)
    total_pages = data['paginationInfo']['num_pages']
    json_data = get_paginated_data_arr(base_url+'?', header, params, 'events', total_pages, 'pageNum')
    df = clean_fetched_data(json_data)
    conn = get_db_connection('someval','1433','same_val','somePass','master')
    try:
        cursor = conn.cursor()
        SQLCommand = ("CREATE DATABASE Events;")
        cursor.execute(SQLCommand)
        print('New database created')
        conn = get_db_connection('someval','1433','same_val','somePass','Events')

        cursor = conn.cursor()
        cmd = """create table events_table ( 
            event_date                Date,
            event_subgroup            varchar(255),
            event_subgroup_display    varchar(255),
            is_today_or_later         varchar(255),
            is_estimate               varchar(255),
            security_id               varchar(255),
            security_name             varchar(255),
            security_url              varchar(255),
            extra                     nvarchar(max),
            currency_code             varchar(255)
            )"""

        cursor.execute(cmd)
        print('created table events_table')
        cmd="""
        alter table events_table
            add constraint [extra record should be formatted as JSON]
                        check (ISJSON(extra)=1)
        """
        cursor.execute(cmd)
        # transform object columns to str
        df.is_estimate = df.is_estimate.astype(str)
        df.is_today_or_later = df.is_today_or_later.astype(str)

        # stringify json
        df.extra = df.extra.apply(lambda x: json.dumps(x))

        # remove single quotes from security_name column
        df.security_name = df.security_name.apply(lambda x: x.replace("'",""))


        chunked_df = get_chunked_dataframe(1000, df)

        # loop over list of dataframes. for eg: if 2000 rows are present 
        # in dataframe and max limit is 1000, it will loop twice

        for dataframe in chunked_df:
            # convert dataframe rows into tuple list for insert query
            data_tupl_list = list(dataframe.to_records(index=False))
            # join list entries to single string for insert query
            val_str = ','.join([str(i) for i in data_tupl_list])
            print(val_str)

            cmd = 'insert into events_table values '+val_str

            cursor.execute(cmd)
        print('all entries added to database')


        cursor.execute("""select security_name, json_value(extra, '$.fiscal_year'),
        json_value(extra, '$.fiscal_quarter')
        from events_table
        where ISJSON(extra) > 0
        and json_value(extra, '$.fiscal_quarter') = 'Q3'
        and  event_subgroup='earnings_results'
        order by event_date""")

        data = cursor.fetchall()
        print(data)
    except Exception as e:
        print('Error: '+str(e))
