### Dashboard Alternative

The goal is to create a harvest report for all data.json harvest sources. This should be a csv and be a stand-in for [our current application](https://labs.data.gov/dashboard/offices/qa).


Here is a potential endpoint for [pulling all harvest sources](https://catalog.data.gov/api/3/action/package_search?rows=10000&q=type:harvest%20source_type:datajson).


Here is an endpoint example for [one harvest source](https://catalog.data.gov/api/3/action/package_search?q=harvest_source_id:55670d71-b811-4fef-9601-97ff5fcc4ae7).

Here's the [CKAN API documentation](https://docs.ckan.org/en/ckan-2.3.5/api/index.html)


### Requirements

- Python 3.8
- pipenv


### CSV Field Explanations

The crawl will populate a csv **only** if it has datasets that can be found with the api.

To run the crawl: `pipenv run crawl.py`

- **org_name** : The name provided in `title`

- **harvest_id** : The id provided in `id`

- **last_modified_date** : The data provided in `metadata_modified`

- **availability** : The opposite of `private`

- **number_of_datasets** : The total datasets per harvest (Note: this does not check if the dataset has )

- **resource_type(s)** : Each unique `mimetype` for the different resources

- **resource_urls** : 5 unique resource url's. Note, this is included because we can do more with these urls. Only 5 used for csv formatting

- **average_openness** : If the dataset has a `qa` field, then this is the average of the `openness_score`. Otherwise is null.

- **harvest_source_title**: Comes from the `extras` dictionary. This could be used for many other dashboard specific entities.

- **last_crawl**: The date of the last crawl
