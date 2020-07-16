import requests
import pandas as pd
import json
import numpy as np
from datetime import datetime
######################################
# this will be a 3-step process with the end goal to be a producable csv
# 1) get a list of all harvest sources
# 2) create json of harvest source
# 3) process harvest.json data
#####################################

# TODO: add arguments
# TODO: add logging


# step 1 - get all harvest sources
def get_all_sources(dataset_number):
    try:
        # setup request and process results in json
        base_url = """
        https://catalog.data.gov/api/3/action/package_search?rows=10000&q=type:harvest%20source_type:datajson
        """
        response = requests.get(base_url)
        body = json.loads(response.text)
        # only begin processing with successful request
        if response.status_code == 200 and body['success']:
            print('Getting {} harvest sources'.format(body['result']['count']))
            # json harvest sources
            harvest_sources = body['result']['results']
            all_ids = []
            for i in range(0, dataset_number):
                all_ids.append(harvest_sources[i]['id'])
            print('We have {} ids ready for processing'.format(len(all_ids)))
            return all_ids
    except Exception as e:
        print('Error getting harvest sources: {}'.format(e))


# step 2 - get harvest json
def process_harvest_info(id):
    try:
        # edit this number to declare the max amounts of dataset results
        max_results = 10000
        # make request and process body as json
        base_url = 'https://catalog.data.gov/api/3/action/package_search'
        parameter = '?rows={}&q=harvest_source_id:{}'.format(max_results, id)
        query = base_url + parameter
        response = requests.get(query)
        body = json.loads(response.text)
        # total datasets
        count = body['result']['count']
        # only process with successful request
        if (response.status_code == 200) and (body['success']):
            harvest_source = body['result']['results']
            # some harvest sources have 0 datasets
            if (len(harvest_source) == 0):
                print('No harvest information for {}. Skipping...'.format(id))
                return
            # Some harvest have a lot of datasets
            elif (count > max_results):
                # TODO, build out the iteration of this section
                print('Lots of results for id {}. Total: {}'.format(id, count))
            else:
                # harvest id json to be consumed by process_dataset_info
                return harvest_source
        else:
            print('Error getting harvest information for id: {}'.format(id))
    # a catch-all for errors
    except Exception as e:
        print('Error getting info for id: {}. Error: {}'.format(id, e))


def process_dataset_info(harvest_source):
    try:
        total_datasets = len(harvest_source)
        print('Processing {} total datasets'.format(total_datasets))
        # iterate through each dataset and get aggregated information
        resource_types = []
        resource_urls = []
        openness_score = []
        for i in range(len(harvest_source)):  # each dataset
            for j in range(len(harvest_source[i]['resources'])):
                resource = harvest_source[i]['resources'][j]  # each res
                # let's only get the unique values
                if resource['mimetype'] not in resource_types:
                    resource_types.append(resource['mimetype'])
                if resource['url'] not in resource_urls:
                    resource_urls.append(resource['url'])
                # the 'qa' field is being read as a string, if present at all
                if 'qa' in resource.keys():
                    idx = resource['qa'].find("'openness_score':")
                    # grab the score and make it an int
                    openness_score.append(int(resource['qa'][idx+18]))
        # get initial info, then some others will be gathered via aggregation
        extracted_harvest_info = [
            harvest_source[0]['organization']['title'],
            harvest_source[0]['id'],
            harvest_source[0]['metadata_modified'],
            not harvest_source[0]['private'],
            total_datasets,
            resource_types,
            resource_urls[:5]  # TODO: change/iterate
        ]
        # can't get the average of a
        if len(openness_score) > 0:
            extracted_harvest_info.append(np.mean(openness_score))
        else:
            extracted_harvest_info.append(np.nan)  # append null if not a value
        # get harvest source title
        # there can be several 'harvest_source_title' in extras
        if 'extras' in harvest_source[0].keys():
            # just get the extra dictionary
            extras = harvest_source[0]['extras']
            harvest_source_title = []
            for i in range(len(extras)):
                if extras[i]['key'] == 'harvest_source_title':
                    harvest_source_title.append(extras[i]['value'])
        # append no harvest title if not in extras
        harvest_source_title.append('No harvest title')
        # take either harvest source name or none
        extracted_harvest_info.append(harvest_source_title[0])
        return extracted_harvest_info
    except TypeError:
        print('Got a type error, but it was handled')
        return None


# parameter number_to_process (int) - number of harvests to process
def create_csv(number_to_process):
    try:
        # this will be the list with our final df
        final_df = []
        ids = get_all_sources(number_to_process)  # get all harvest source ids
        for id in ids:
            harvest_source = process_harvest_info(id)  # get json of harvest
            new_row = process_dataset_info(harvest_source)
            if new_row:  # handle none
                final_df.append(new_row)
        # here's our df to be turned into a csv
        df = pd.DataFrame(final_df, columns=[
                                'org_name',
                                'harvest_id',
                                'last_modified_date',
                                'availability',
                                'number_of_datasets',
                                'resource_type(s)',
                                'resource_urls',  # TODO: better CSV formatting
                                'average_openness',
                                'harvest_source_title'])
        # add today's date
        df['last_crawl'] = datetime.now()
        df.to_csv('crawl_results.csv', index=False)
        print('CSV created successsfully with {} rows.'.format(len(df)))
    except Exception as e:
        print('An error occurred. Error: {}'.format(e))


# create our df, edit the number to process the number of datasets
create_csv(150)
