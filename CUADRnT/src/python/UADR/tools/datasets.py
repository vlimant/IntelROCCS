#!/usr/bin/env python2.7
"""
File       : datasets.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Handle dataset data
"""

# system modules
import logging
import threading
import Queue
import datetime

# package modules
from UADR.utils.utils import timestamp_to_datetime
from UADR.utils.utils import datetime_day
from UADR.utils.utils import get_json
from UADR.services.phedex import PhEDExService
from UADR.services.dbs import DBSService
from UADR.tools.sites import SiteManager
from UADR.tools.popularity import PopularityManager
from UADR.tools.storage import StorageManager

class DatasetManager(object):
    """
    Handle all dataset related data
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.phedex = PhEDExService(self.config)
        self.dbs = DBSService(self.config)
        self.storage = StorageManager(self.config)
        self.sites = SiteManager(self.config)
        self.popularity = PopularityManager(self.config)
        self.MAX_THREADS = int(config['threading']['max_threads'])
        self.start_date = datetime_day(datetime.datetime.utcnow())

    def initiate_db(self):
        """
        Initiate dataset data in database
        Get general data and popularity data from beginning
        """
        q = Queue.Queue()
        for i in range(self.MAX_THREADS):
            worker = threading.Thread(target=self.insert_dataset_data, args=(i, q))
            worker.daemon = True
            worker.start()
        active_sites = self.sites.get_active_sites()
        api = 'blockreplicas'
        params = [('node', active_sites), ('create_since', 0.0), ('complete', 'y'), ('group', 'AnalysisOps'), ('show_dataset', 'y')]
        phedex_data = self.phedex.fetch(api=api, params=params)
        for dataset_data in self.generate_dataset_data(phedex_data):
            q.put(get_json(dataset_data, 'name'))
        self.logger.info('Done inserting datasets into DB')
        q.join()

    def generate_dataset_data(self, phedex_data):
        """
        Generator function to get every dataset in system
        """
        for dataset_data in get_json(get_json(phedex_data, 'phedex'), 'dataset'):
            yield dataset_data

    def insert_dataset_data(self, i, q):
        """
        Insert a new dataset into the database and initiate all data
        """
        count = 1
        while True:
            dataset_name = q.get()
            self.logger.info('Inserting dataset number %d', count)
            date = self.start_date
            coll = 'dataset_data'
            data = [{'name':dataset_name}]
            self.storage.insert_data(coll=coll, data=data)
            self.insert_phedex_data(dataset_name)
            self.insert_dbs_data(dataset_name)
            self.popularity.initiate_db(dataset_name, end_date=date)
            count += 1
            q.task_done()

    def insert_phedex_data(self, dataset_name):
        """
        Fetch phedex data about dataset and insert into database
        """
        api = 'data'
        params = {'dataset':dataset_name, 'level':'block', 'create_since':0.0}
        phedex_data = self.phedex.fetch(api=api, params=params)
        size_bytes = 0
        n_files = 0
        for block_data in get_json(get_json(get_json(get_json(get_json(get_json(phedex_data, 'phedex'), 'dbs'), 0), 'dataset'), 0), 'block'):
            size_bytes += get_json(block_data, 'bytes')
            n_files += get_json(block_data, 'files')
        coll = 'dataset_data'
        query = {'name':dataset_name}
        data = {'$set':{'size_bytes':size_bytes, 'n_files':n_files}}
        self.storage.update_data(coll=coll, query=query, data=data, upsert=False)

    def insert_dbs_data(self, dataset_name):
        """
        Fetch dbs data about dataset and insert into database
        """
        api = 'datasets'
        params = {'dataset':dataset_name, 'detail':True, 'dataset_access_type':'*'}
        dbs_data = self.dbs.fetch(api=api, params=params)
        dataset_data = dbs_data['data'][0]
        ds_name = get_json(dataset_data, 'primary_ds_name')
        physics_group = get_json(dataset_data, 'physics_group_name')
        data_tier = get_json(dataset_data, 'data_tier_name')
        creation_date = datetime_day(timestamp_to_datetime(get_json(dataset_data, 'creation_date')))
        ds_type = get_json(dataset_data, 'primary_ds_type')
        coll = 'dataset_data'
        query = {'name':dataset_name}
        data = {'$set':{'ds_name':ds_name, 'physics_group':physics_group, 'data_tier':data_tier, 'creation_date':creation_date, 'ds_type':ds_type}}
        self.storage.update_data(coll=coll, query=query, data=data, upsert=False)

    def update_db(self):
        """
        Get datasets currently in AnalysisOps and compare to database
        Deactivate removed datasets and insert new
        Update replicas
        """
        # get all datasets in database
        dataset_names = self.get_db_datasets()
        self.popularity.update_db(dataset_names)
        dataset_names = set(dataset_names)
        # get all active sites, only fetch replicas from these
        active_sites = self.sites.get_active_sites()
        api = 'blockreplicas'
        params = [('node', active_sites), ('create_since', 0.0), ('complete', 'y'), ('group', 'AnalysisOps'), ('show_dataset', 'y')]
        phedex_data = self.phedex.fetch(api=api, params=params)
        current_datasets = set()
        date = datetime_day(datetime.datetime.utcnow())
        for dataset_data in self.generate_datasets(phedex_data):
            dataset_name = get_json(dataset_data, 'name')
            current_datasets.add(dataset_name)
            if dataset_name not in dataset_names:
                # this is a new dataset which need to be inserted into the database
                self.insert_dataset_data(dataset_name, date)
            # update replicas
            replicas = self.generate_replicas(dataset_data)
            coll = 'dataset_data'
            query = {'name':dataset_name}
            data = {'$set':{'replicas':list(replicas)}}
            data = self.storage.update_data(coll=coll, query=query, data=data, upsert=False)
        deprecated_datasets = dataset_names - current_datasets
        for dataset_name in deprecated_datasets:
            self.remove_dataset(dataset_name)

    def get_db_datasets(self):
        """
        Get all datasets currently in database
        """
        coll = 'dataset_data'
        pipeline = list()
        project = {'$project':{'name':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        dataset_names = [dataset_data['name'] for dataset_data in data]
        self.logger.info('%d datasets present in database', len(dataset_names))
        return dataset_names

    def generate_replicas(self, dataset_data):
        """
        Generator function to get all replicas of a dataset
        """
        for block_data in get_json(dataset_data, 'block'):
            for replica_data in get_json(block_data, 'replica'):
                if get_json(replica_data, 'files') > 0:
                    yield get_json(replica_data, 'node')

    def remove_dataset(self, dataset_name):
        """
        Remove dataset from database
        """
        coll = 'dataset_data'
        query = {'name':dataset_name}
        self.storage.delete_data(coll=coll, query=query)

    # def get_sites(self, dataset_name):
    #     """
    #     Get all sites with a replica of the dataset
    #     """
    #     coll = 'dataset_data'
    #     pipeline = list()
    #     match = {'$match':{'name':dataset_name}}
    #     pipeline.append(match)
    #     project = {'$project':{'replicas':1, '_id':0}}
    #     pipeline.append(project)
    #     data = self.storage.get_data(coll=coll, pipeline=pipeline)
    #     site_names = data[0]['replicas']
    #     return site_names

    # def get_size(self, dataset_name):
    #     """
    #     Get size in GB of dataset
    #     """
    #     coll = 'dataset_data'
    #     pipeline = list()
    #     match = {'$match':{'name':dataset_name}}
    #     pipeline.append(match)
    #     project = {'$project':{'size_bytes':1, '_id':0}}
    #     pipeline.append(project)
    #     data = self.storage.get_data(coll=coll, pipeline=pipeline)
    #     size_gb = float(data[0]['size_bytes'])/10**9
    #     return size_gb