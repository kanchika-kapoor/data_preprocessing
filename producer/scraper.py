import requests
from urllib.parse import urlencode


class ScrapData():
    
    def __init__(self, headers):
        self.headers = headers
    
    def dict_to_query_params(self, params):
        """
        method for dictionary to query param convertion
        returns url encoded string
        """
        return urlencode(params)

    def get_json_response(self,url):
        """
        calls the url with given headers using get
        returns json response
        """
        try:
            resp = requests.get(url, headers=self.headers)
            data = resp.json()
            return data
        except Exception as e:
            print('error: '+str(e))

    def get_paginated_data_arr(self,url,params,data_key_name,total_pages_num,curr_page_num_key):
        """
        loops till total_pages_num and fetches data for every page
        page number is taken through query param of url, so we build 
        the url in every step and fetch the data
        returns a list containing all data
        """
        try:
            total_arr = []
            for i in range(1, total_pages_num+1):
                params[curr_page_num_key]=i
                url = url + self.dict_to_query_params(params)
                data = self.get_json_response(url)[data_key_name]
                total_arr += data
            return total_arr
        except Exception as e:
            print('error: '+str(e))
