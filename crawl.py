import requests
import pandas as pd
import json
import sys
import os
#load api_key, though may not be necessary
from dotenv import load_dotenv
load_dotenv()
api_key = os.getenv('api_key')
######################################
# this will be a 2-step process (min) with the end goal to be a producable csv
# 1) get a list of all harvest sources
# 2) gather information from harvest source and create csv
# 3) MAYBE: create visualization from data
#####################################

#step 1 - get all harvest sources
def get_all_sources():
    try:
        base_url = 'https://catalog.data.gov/api/3/action/package_search'
        params = {'type' : 'datajson'}
        headers = {'Authorization' : api_key}
        response = requests.get(base_url, params=params, headers=headers, timeout=0.01)
        body = json.loads(response.text)
        if response.status_code == 200 and body['success']:
            print(body['result'])
    except:
        print('Error getting harvest sources: {}'.format(sys.exc_info()))

get_all_sources()
