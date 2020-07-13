import requests
import pandas as pd
import json
import sys
import os
#load api_key, though may not be necessary
from dotenv import load_dotenv
import numpy as np
load_dotenv()
api_key = os.getenv('api_key')
######################################
# this will be a 2-step process (min) with the end goal to be a producable csv
# 1) get a list of all harvest sources
# 2) gather information from harvest source and create csv
# 3) MAYBE: create visualization from data
#####################################

# TODO: add arguments
# TODO: add logging


#step 1 - get all harvest sources
def get_all_sources(dataset_number):
    try:
        #setup request and process results in json
        base_url = 'https://catalog.data.gov/api/3/action/package_search?rows=10000&q=type:harvest%20source_type:datajson'
        response = requests.get(base_url)
        body = json.loads(response.text)
        #only begin processing with successful request
        if response.status_code == 200 and body['success']:
            print('Getting {} harvest sources'.format(body['result']['count']))
            #response formatting is strange, but this is the list of all harvest sources
            harvest_sources = body['result']['results']
            print('Using only the first {} harvest sources'.format(dataset_number))
            all_ids = []
            for i in range(0, dataset_number):
                all_ids.append(harvest_sources[i]['id'])
            print('We have {} ids ready for processing'.format(len(all_ids)))
            return all_ids
    except:
        print('Error getting harvest sources: {}'.format(sys.exc_info()))

def process_harvest_info(id):
    try:
        #edit this number to declare the max amounts of harvest results to attempt with each request
        max_results = 10000
        #make request and process body as json
        base_url = 'https://catalog.data.gov/api/3/action/package_search?rows={}&q=harvest_source_id:{}'.format(max_results, id)
        response = requests.get(base_url)
        body = json.loads(response.text)
        #only process with successful request
        if (response.status_code == 200) and (body['success']):
            harvest_source = body['result']['results']
            #some harvest sources have 0 datasets
            if (len(harvest_source) == 0):
                print('No harvest information for {}. Skipping...'.format(id))
                return
            #Some harvest have a lot of datasets
            elif (body['result']['count'] > max_results):
                #TO DO, build out the iteration of this section
                print('Lots of results for id {}. Total: {}'.format(id, body['result']['count']))
            else:
                # harvest id json to be consumed by process_dataset_info
                return harvest_source
        else:
            print('Error getting harvest information for harvest source id: {}'.format(id))
    #a catch-all for errors
    except:
        print('Error getting info for harvest source: {}. Error: {}'.format(id, sys.exc_info()))


def process_dataset_info(harvest_source):
    try:
        total_datasets = len(harvest_source)
        # list to append to final df
        print('Processing {} total datasets'.format(total_datasets))
        # iterate through each dataset and get aggregated information
        resource_types = []
        resource_urls = []
        openness_score = []
        for i in range(len(harvest_source)): #each dataset
                for j in range(len(harvest_source[i]['resources'])): #each datasets' resources
                    resource = harvest_source[i]['resources'][j]
                    #let's only get the unique values
                    if resource['mimetype'] not in resource_types:
                        resource_types.append(resource['mimetype'])
                    if resource['url'] not in resource_urls:
                        resource_urls.append(resource['url'])
                    #the 'qa' field is being read as a string, if present at all
                    if 'qa' in resource.keys():
                        idx = resource['qa'].find("'openness_score':")
                        openness_score.append(int(resource['qa'][idx+18]))
        #append to our list
        # get initial info, then some others will be gathered via aggregation
        extracted_harvest_info = [
        harvest_source[0]['organization']['title'],
        harvest_source[0]['id'],
        harvest_source[0]['metadata_modified'],
        not harvest_source[0]['private'],
        total_datasets,
        resource_types,
        resource_urls[:5] #some have a lot of urls (only keep the first few)
        ]
        # can't get the average of a
        if len(openness_score) > 0:
            extracted_harvest_info.append(np.mean(openness_score))
        else:
            extracted_harvest_info.append(np.nan) #append null if not a value
        return extracted_harvest_info
    except TypeError:
        print('Got a type error, but it was handled')
        return None

#parameter number_to_process should be defined as the most you want to produce
def create_csv(number_to_process):
    try:
        # this will be the list with our final df
        final_df = []
        ids = get_all_sources(number_to_process) #get all harvest source ids
        for id in ids:
            harvest_source = process_harvest_info(id) #get json of harvest sources
            new_row = process_dataset_info(harvest_source)
            if new_row: #handle none
                final_df.append(new_row)
        #here's our df to be turned into a csv
        df = pd.DataFrame(final_df, columns=[
                                'org_name',
                                'harvest_id',
                                'last_modified_date',
                                'availability',
                                'number_of_datasets',
                                'resource_type(s)',
                                'resource_urls',
                                'average_openness'])

        df.to_csv('crawl_results.csv', index=False)
        print('CSV created successsfully with {} rows.'.format(len(df)))
    except:
        print('An error occurred. Error: {}'.format(sys.exc_info()))


#create our df
create_csv(10)
