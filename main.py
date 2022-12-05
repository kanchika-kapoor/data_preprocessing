from producer.scraper import ScrapData
from transformer.data_transformer import TransformData
from connector.connector import DBConnector
import json

if __name__ == '__main__':
    # extract data
    header = {
          'Accept': 'application/json',
          'Content':'application/json',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:107.0) Gecko/20100101 Firefox/107.0'
    }
    # start and end date can be configured to any range
    start_date = '11/20/2022'
    end_date = '11/25/2022'
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

    base_url = 'https://ycharts.com/events/events_data/json?'

    producer = ScrapData(header)

    url = base_url+producer.dict_to_query_params(params)
    print('fetching data from source')

    data = producer.get_json_response(url)
    total_pages = data['paginationInfo']['num_pages']
    json_data = producer.get_paginated_data_arr(base_url, params, 'events', total_pages, 'pageNum')


    # transform data
    transformer = TransformData(json_data)
    df = transformer.json_to_dataframe()
    print('converted json to dataframe:')
    print(df)
    df = transformer.drop_cols(['event_date_fmtd', 'event_date_js', 'event_datetime_utc_js','event_time_fmtd','description'], df)
    # not all day ranges have currency code data
    if 'currency_code' in df.columns:
      df.currency_code = df.currency_code.fillna('Unknown')
    else:
      df['currency_code']='Unknown'
    # transform object columns to str
    df.is_estimate = transformer.change_col_type(df.is_estimate, str)
    df.is_today_or_later = transformer.change_col_type(df.is_today_or_later,str)
    df.extra = df.extra.apply(lambda x: json.dumps(x))
    # remove single quotes from security_name column
    df.security_name = df.security_name.apply(lambda x: x.replace("'",""))
    chunked_df = transformer.get_chunked_dataframe(1000, df)


    # load data
    host = '20.101.66.12'
    port='1443'
    user = 'sa'
    password = 'admin@1234'
    db = 'master'

    connector = DBConnector()
    conn = connector.get_db_connection(host,port,user,password,db)

    try:
        # create new db
        cursor = conn.cursor()
        SQLCommand = ("CREATE DATABASE Events;")
        cursor.execute(SQLCommand)
        print('Events database created')
        conn.close()

        conn = connector.get_db_connection(host,port,user,password,'Events')

        cursor = conn.cursor()
        # schema
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
        # constraint
        cmd="""
        alter table events_table
            add constraint [extra record should be formatted as JSON]
                        check (ISJSON(extra)=1)
        """
        cursor.execute(cmd)


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

        query = """select security_name, json_value(extra, '$.fiscal_year') as fiscal_year,
        json_value(extra, '$.fiscal_quarter') as quarter
        from events_table
        where ISJSON(extra) > 0
        and json_value(extra, '$.fiscal_quarter') = 'Q3'
        and  event_subgroup='earnings_results'
        order by event_date"""

        cursor.execute(query)

        data = cursor.fetchall()
        print(data)

        df_query = transformer.sql_to_df(conn, query)
        print(df_query)
    except Exception as e:
        print('Error: '+str(e))
        pass
