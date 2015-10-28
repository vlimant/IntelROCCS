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
from cuadrnt.utils.utils import timestamp_to_datetime
from cuadrnt.utils.utils import datetime_day
from cuadrnt.utils.utils import get_json
from cuadrnt.data_management.services.phedex import PhEDExService
from cuadrnt.data_management.services.dbs import DBSService
from cuadrnt.data_management.tools.sites import SiteManager
from cuadrnt.data_management.core.storage import StorageManager

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
        self.MAX_THREADS = int(config['threading']['max_threads'])

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
        params = [('node', active_sites), ('create_since', 0.0), ('complete', 'y'), ('dist_complete', 'y'), ('group', 'AnalysisOps'), ('show_dataset', 'y')]
        t1 = datetime.datetime.utcnow()
        phedex_data = self.phedex.fetch(api=api, params=params)
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('Call to PhEDEx took %s', str(td))
        count = 1
        t1 = datetime.datetime.utcnow()
        for dataset_data in get_json(get_json(phedex_data, 'phedex'), 'dataset'):
            q.put((dataset_data, count))
            count += 1
        q.join()
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('Inserting dataset data took %s', str(td))
        self.logger.info('Done inserting datasets into DB')

    def update_db(self):
        """
        Get datasets currently in AnalysisOps and compare to database
        Deactivate removed datasets and insert new
        Update replicas
        """
        # get all datasets in database
        dataset_names = self.get_db_datasets()
        dataset_names = set(dataset_names)
        # get all active sites, only fetch replicas from these
        active_sites = self.sites.get_active_sites()
        api = 'blockreplicas'
        params = [('node', active_sites), ('create_since', 0.0), ('complete', 'y'), ('group', 'AnalysisOps'), ('show_dataset', 'y')]
        t1 = datetime.datetime.utcnow()
        phedex_data = self.phedex.fetch(api=api, params=params)
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('Call to PhEDEx took %s', str(td))
        current_datasets = set()
        q = Queue.Queue()
        for i in range(self.MAX_THREADS):
            worker = threading.Thread(target=self.insert_dataset_data, args=(i, q))
            worker.daemon = True
            worker.start()
        count = 1
        t1 = datetime.datetime.utcnow()
        for dataset_data in get_json(get_json(phedex_data, 'phedex'), 'dataset'):
            dataset_name = get_json(dataset_data, 'name')
            current_datasets.add(dataset_name)
            if dataset_name not in dataset_names:
                # this is a new dataset which need to be inserted into the database
                q.put((dataset_data, count))
                count += 1
            else:
                # update replicas
                replicas = self.get_replicas(dataset_data)
                coll = 'dataset_data'
                query = {'name':dataset_name}
                data = {'$set':{'replicas':replicas}}
                data = self.storage.update_data(coll=coll, query=query, data=data, upsert=False)
        q.join()
        deprecated_datasets = dataset_names - current_datasets
        for dataset_name in deprecated_datasets:
            self.remove_dataset(dataset_name)
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('Updating dataset data took %s', str(td))
        self.logger.info('Done updating datasets in DB')

    def insert_dataset_data(self, i, q):
        """
        Insert a new dataset into the database and initiate all data
        """
        while True:
            data = q.get()
            dataset_data = data[0]
            count = data[1]
            self.logger.debug('Inserting dataset number %d', count)
            dataset_name = get_json(dataset_data, 'name')
            coll = 'dataset_data'
            query = {'name':dataset_name}
            data = {'$set':{'name':dataset_name}}
            data = self.storage.update_data(coll=coll, query=query, data=data, upsert=True)
            try:
                self.insert_phedex_data(dataset_name)
                self.insert_dbs_data(dataset_name)
                replicas = self.get_replicas(dataset_data)
                query = {'name':dataset_name}
                data = {'$set':{'name':dataset_name, 'replicas':replicas}}
            except:
                coll = 'dataset_data'
                query = {'name':dataset_name}
                self.storage.delete_data(coll=coll, query=query)
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
        dataset_data = get_json(get_json(get_json(phedex_data, 'phedex'), 'dbs')[0],'dataset')[0]
        for block_data in get_json(dataset_data, 'block'):
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
        dataset_data = get_json(dbs_data, 'data')[0]
        ds_name = get_json(dataset_data, 'primary_ds_name')
        physics_group = get_json(dataset_data, 'physics_group_name')
        data_tier = get_json(dataset_data, 'data_tier_name')
        creation_date = datetime_day(timestamp_to_datetime(get_json(dataset_data, 'creation_date')))
        ds_type = get_json(dataset_data, 'primary_ds_type')
        coll = 'dataset_data'
        query = {'name':dataset_name}
        data = {'$set':{'ds_name':ds_name, 'physics_group':physics_group, 'data_tier':data_tier, 'creation_date':creation_date, 'ds_type':ds_type}}
        self.storage.update_data(coll=coll, query=query, data=data, upsert=False)

    def get_replicas(self, dataset_data):
        """
        Generator function to get all replicas of a dataset
        """
        replicas_check = dict()
        dataset_name = get_json(dataset_data, 'name')
        for block_data in get_json(dataset_data, 'block'):
            for replica_data in get_json(block_data, 'replica'):
                try:
                    replicas_check[get_json(replica_data, 'node')] += get_json(replica_data, 'files')
                except:
                    replicas_check[get_json(replica_data, 'node')] = get_json(replica_data, 'files')
        replicas = list()
        n_files = self.get_n_files(dataset_name)
        for site, site_files in replicas_check.items():
            if site_files == n_files:
                replicas.append(site)
        return replicas

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

    def remove_dataset(self, dataset_name):
        """
        Remove dataset from database
        """
        coll = 'dataset_data'
        query = {'name':dataset_name}
        self.storage.delete_data(coll=coll, query=query)

    def get_n_files(self, dataset_name):
        """
        Get the number of files in the block
        """
        coll = 'dataset_data'
        pipeline = list()
        match = {'$match':{'name':dataset_name}}
        pipeline.append(match)
        project = {'$project':{'n_files':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        return data[0]['n_files']

    def get_dataset_features(self, dataset_name):
        """
        Get dataset features for dataset from db
        """
        coll = 'dataset_data'
        pipeline = list()
        match = {'$match':{'name':dataset_name}}
        pipeline.append(match)
        project = {'$project':{'dataset_name':'$name', 'size_gb':{'$multiply':['$size_bytes', 0.000000001]}, 'n_files':1, 'physics_group':1, 'ds_type':1, 'data_tier':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        return data[0]

    def get_current_num_replicas(self):
        """
        Get the current number of replicas for all replicas
        """
        coll = 'dataset_data'
        pipeline = list()
        group = {'$group':{'_id':'$name', 'n_replicas':{'$first':{'$size':{'$replicas'}}}}}
        pipeline.append(group)
        project = {'$project':{'name':'$_id', 'n_replicas':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        return data

    def get_sites(self, dataset_name):
        """
        Get all sites with a replica of the dataset
        """
        coll = 'dataset_data'
        pipeline = list()
        match = {'$match':{'name':dataset_name}}
        pipeline.append(match)
        project = {'$project':{'replicas':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        site_names = data[0]['replicas']
        return site_names

    def get_size(self, dataset_name):
        """
        Get size in GB of dataset
        """
        coll = 'dataset_data'
        pipeline = list()
        match = {'$match':{'name':dataset_name}}
        pipeline.append(match)
        project = {'$project':{'size_bytes':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        size_gb = float(data[0]['size_bytes'])/10**9
        return size_gb
