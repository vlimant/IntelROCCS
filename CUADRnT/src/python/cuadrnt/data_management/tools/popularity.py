#!/usr/bin/env python2.7
"""
File       : popularity.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Generate popularity metric
"""

# system modules
import logging
import datetime
import Queue
import threading
from math import log
import numpy as np

# package modules
# from cuadrnt.utils.utils import pop_db_timestamp_to_datetime
from cuadrnt.utils.utils import datetime_to_string
from cuadrnt.utils.utils import datetime_day
from cuadrnt.utils.utils import pop_db_timestamp_to_datetime
from cuadrnt.utils.utils import daterange
from cuadrnt.utils.utils import get_json
from cuadrnt.data_management.services.pop_db import PopDBService
from cuadrnt.data_management.tools.sites import SiteManager
from cuadrnt.data_management.tools.datasets import DatasetManager
from cuadrnt.data_management.core.storage import StorageManager

class PopularityManager(object):
    """
    Generate popularity metrics for datasets and sites
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.pop_db = PopDBService(self.config)
        self.sites = SiteManager(self.config)
        self.datasets = DatasetManager(self.config)
        self.storage = StorageManager(self.config)
        self.MAX_THREADS = int(config['threading']['max_threads'])

    def initiate_db(self):
        """
        Collect popularity data
        """
        q = Queue.Queue()
        for i in range(self.MAX_THREADS):
            worker = threading.Thread(target=self.insert_popularity_data, args=(i, q))
            worker.daemon = True
            worker.start()
        start_date = datetime_day(datetime.datetime.utcnow() - datetime.timedelta(days=90))
        end_date = datetime_day(datetime.datetime.utcnow())
        # fetch popularity data
        t1 = datetime.datetime.utcnow()
        for date in daterange(start_date, end_date):
            q.put(date)
        q.join()
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('Inserting Pop DB data took %s', str(td))

    def insert_popularity_data(self, i, q):
        """
        Insert popularity data for one dataset into db
        """
        coll = 'dataset_popularity'
        while True:
            date = q.get()
            self.logger.info('Inserting date %s', datetime_to_string(date))
            api = 'DSStatInTimeWindow/'
            tstart = datetime_to_string(date)
            tstop = tstart
            params = {'sitename':'summary', 'tstart':tstart, 'tstop':tstop}
            json_data = self.pop_db.fetch(api=api, params=params)
            # sort it in dictionary for easy fetching
            for dataset in json_data['DATA']:
                dataset_name = dataset['COLLNAME']
                popularity_data = {'name':dataset_name, 'date':date}
                popularity_data['n_accesses'] = dataset['NACC']
                popularity_data['n_cpus'] = dataset['TOTCPU']
                popularity_data['n_users'] = dataset['NUSERS']
                query = {'name':dataset_name, 'date':date}
                data = {'$set':popularity_data}
                self.storage.update_data(coll=coll, query=query, data=data, upsert=True)
            q.task_done()

    def update_db(self):
        """
        Fetch latest popularity data not in database
        """
        # get dates
        coll = 'dataset_popularity'
        pipeline = list()
        sort = {'$sort':{'date':-1}}
        pipeline.append(sort)
        limit = {'$limit':1}
        pipeline.append(limit)
        project = {'$project':{'date':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        try:
            start_date = data[0]['date']
        except:
            self.logger.warning('Popularity needs to be initiated')
            self.initiate_db()
            return
        q = Queue.Queue()
        for i in range(self.MAX_THREADS):
            worker = threading.Thread(target=self.insert_popularity_data, args=(i, q))
            worker.daemon = True
            worker.start()
        start_date = start_date + datetime.timedelta(days=1)
        end_date = datetime_day(datetime.datetime.utcnow())
        # fetch popularity data
        t1 = datetime.datetime.utcnow()
        for date in daterange(start_date, end_date):
            q.put(date)
        q.join()
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('Updating Pop DB data took %s', str(td))

    def insert_dataset(self, dataset_name):
        """
        Fetch all popularity data for dataset
        """
        api = 'getSingleDSstat'
        sitename = 'summary'
        name = dataset_name
        aggr = 'day'
        orderbys = ['totcpu', 'naccess']
        coll = 'dataset_popularity'
        for orderby in orderbys:
            params = {'sitename':sitename, 'name':name, 'aggr':aggr, 'orderby':orderby}
            json_data = self.pop_db.fetch(api=api, params=params)
            data = get_json(json_data, 'data')
            for pop_data in get_json(data, 'data'):
                date = pop_db_timestamp_to_datetime(pop_data[0])
                query = {'name':dataset_name, 'date':date}
                popularity_data = {'name':dataset_name, 'date':date}
                popularity_data[orderby] = pop_data[1]
                data = {'$set':popularity_data}
                self.storage.update_data(coll=coll, query=query, data=data, upsert=True)

    def get_date_popularity(self, dataset_name, date):
        """
        Fetch all popularity data for dataset
        """
        coll = 'dataset_popularity'
        pipeline = list()
        match = {'$match':{'name':dataset_name, 'date':date}}
        pipeline.append(match)
        project = {'$project':{'n_cpus':1, 'n_accesses':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        try:
            popularity = log(data[0]['n_cpus']*data[0]['n_accesses'])
        except:
            popularity = 0.0
        return popularity

    def get_all_dataset_popularity(self, dataset_names, date):
        """
        Fetch popularity for all datasets
        """
        start_date = date - datetime.timedelta(days=6)
        end_date = date
        coll = 'dataset_popularity'
        pipeline = list()
        match = {'$match':{'name':{'$in':dataset_names}, 'date':{'$gte':start_date, '$lte':end_date}}}
        pipeline.append(match)
        project = {'$project':{'name':1, 'n_cpus':1, 'n_accesses':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        popularity = dict()
        for dataset in data:
            try:
                val = log(dataset['n_cpus']*dataset['n_accesses'])
            except:
                val = 0.0
            try:
                popularity[dataset['name']].append(val)
            except:
                popularity[dataset['name']] = [val]
        for dataset_name in dataset_names:
            if not (dataset_name in popularity):
                popularity[dataset_name] = 0.0
            else:
                for i in range(0, 7):
                    try:
                        popularity[dataset_name][i]
                    except:
                        popularity[dataset_name].append(0.0)
                popularity[dataset_name] = np.mean(popularity[dataset_name])
        return popularity

    def get_all_site_popularity(self, site_names, date):
        """
        Get popularity for site
        """
        sites_popularity = dict()
        start_date = date - datetime.timedelta(days=6)
        end_date = date
        for site_name in site_names:
            # get all datasets with a replica at the site and how many replicas it has
            coll = 'dataset_data'
            pipeline = list()
            match = {'$match':{'replicas':site_name}}
            pipeline.append(match)
            project = {'$project':{'name':1, '_id':0}}
            pipeline.append(project)
            data = self.storage.get_data(coll=coll, pipeline=pipeline)
            dataset_names = [dataset_data['name'] for dataset_data in data]
            # get the popularity of the dataset and decide by number of replicas
            coll = 'dataset_popularity'
            pipeline = list()
            match = {'$match':{'name':{'$in':dataset_names}, 'date':{'$gte':start_date, '$lte':end_date}}}
            pipeline.append(match)
            project = {'$project':{'name':1, 'n_cpus':1, 'n_accesses':1, '_id':0}}
            pipeline.append(project)
            data = self.storage.get_data(coll=coll, pipeline=pipeline)
            popularity = 0.0
            for entry in data:
                try:
                    popularity += log(entry['n_cpus']*entry['n_accesses'])
                except:
                    popularity += 0.0
            performance = self.sites.get_performance(site_name)
            sites_popularity[site_name] = popularity/performance
        return sites_popularity

    def get_dataset_popularity(self, dataset_name):
        """
        Get all popularity data for a dataset
        """
        coll = 'dataset_popularity'
        pipeline = list()
        match = {'$match':{'name':dataset_name}}
        pipeline.append(match)
        project = {'$project':{'date':1, 'n_cpus':1, 'n_accesses':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        return data[0]

    def get_average_popularity(self, dataset_name, date):
        """
        Get all popularity data for a dataset
        """
        start_date = date - datetime.timedelta(days=7)
        end_date = date - datetime.timedelta(days=1)
        coll = 'dataset_popularity'
        pipeline = list()
        match = {'$match':{'name':dataset_name}}
        pipeline.append(match)
        match = {'$match':{'date':{'$gte':start_date, '$lte':end_date}}}
        pipeline.append(match)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        pops = list()
        for i in range(0, 7):
            try:
                pops.append(log(float(data[i]['n_accesses']*data[i]['n_cpus'])))
            except:
                pops.append(0.0)
        avg = np.mean(pops)
        return avg

    def get_features(self,dataset_names, date):
        """
        Get machine learning features which are seven days of popularity
        """
        dataset_features = dict()
        end_date = date
        start_date = end_date - datetime.timedelta(days=7)
        coll = 'dataset_popularity'
        pipeline = list()
        match = {'$match':{'name': {'$in':dataset_names}, 'date':{'$gte':start_date, '$lte':end_date}}}
        pipeline.append(match)
        group = {'$group':{'_id':{'name':'$name', 'date':'$date'}, 'popularity':{'$sum':{'$multiply':['$n_accesses', '$n_cpus']}}}}
        pipeline.append(group)
        group = {'$group':{'_id':'$_id.name', 'features': {'$push':{'date':'$_id.date', 'popularity':'$popularity'}}}}
        pipeline.append(group)
        project = {'$project':{'features':1, '_id':1}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        for dataset in data:
            dataset_name = dataset['_id']
            tmp_features = dict()
            for feat in dataset['features']:
                try:
                    tmp_features[feat['date']] = log(feat['popularity'])
                except:
                    tmp_features[feat['date']] = 0.0
            dataset_features[dataset_name] = list()
            for date in daterange(start_date, end_date):
                try:
                    dataset_features[dataset_name].append(tmp_features[date])
                except:
                    dataset_features[dataset_name].append(0)
        return dataset_features
