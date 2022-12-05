from io import StringIO
import pandas as pd
import json


class TransformData():
    def __init__(self, json_data):
        self.json_data = json_data

    def json_to_dataframe(self):
        """
        convert json data to dataframe
        returns dataframe
        """
        json_data = self.json_data
        json_str = json.dumps(json_data) #convert dictionary to string
        json_buf = StringIO(json_str) # make string buffer

        df = pd.read_json(json_buf)
        return df

    def drop_cols(self, cols_arr, dataframe):
        """
        drop dataframe column
        returns dataframe
        """
        dataframe = dataframe.drop(cols_arr, axis=1)
        return dataframe

    def get_chunked_dataframe(self, max_row_count, dataframe):
        """
        split dataframe into n parts based on max_row_count
        returns list of dataframes
        """
        arr=[]
        chunk_size = round(len(dataframe)/max_row_count)
        for i in range(chunk_size):
            arr.append(dataframe[i*max_row_count:max_row_count*(i+1)])
        return arr

    def change_col_type(self, col, data_type):
        """
        change column data type
        returns dataframe
        """
        col = col.astype(data_type)
        return col

    def sql_to_df(self, conn, query):
        """
        convert query output to dataframe
        returns dataframe
        """
        df = pd.read_sql(query, conn)
        return df