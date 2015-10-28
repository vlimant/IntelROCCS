#!/usr/bin/env python2.7
"""
File       : generic.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Generic class for all ranking algorithms
"""

# system modules
import logging
import datetime
import operator

# package modules
from cuadrnt.utils.utils import datetime_day
from cuadrnt.data_management.tools.sites import SiteManager
from cuadrnt.data_management.tools.datasets import DatasetManager
from cuadrnt.data_management.tools.popularity import PopularityManager
from cuadrnt.data_management.core.storage import StorageManager

class GenericRanking(object):
    """
    Generic Ranking class
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.sites = SiteManager(self.config)
        self.datasets = DatasetManager(self.config)
        self.popularity = PopularityManager(self.config)
        self.storage = StorageManager(self.config)
        self.max_replicas = int(config['rocker_board']['max_replicas'])
        self.name = 'generic'

    def get_dataset_rankings(self):
        """
        Generate dataset rankings
        """
        date = datetime_day(datetime.datetime.utcnow())
        rank_name = self.name + '_rank'
        popularity_name = self.name + '_popularity'
        dataset_names = self.datasets.get_db_datasets()
        dataset_popularity = dict()
        for dataset_name in dataset_names:
            popularity = self.get_dataset_popularity(dataset_name)
            dataset_popularity[dataset_name] = popularity
        dataset_rankings = self.normalize_popularity(dataset_popularity)
        # store in database
        for dataset_name, popularity in dataset_popularity.items():
            rank = dataset_rankings[dataset_name]
            coll = 'dataset_rankings'
            query = data = {'name':dataset_name, 'date':date}
            data = {'$set':{'name':dataset_name, 'date':date, rank_name:rank, popularity_name:popularity}}
            self.storage.update_data(coll=coll, query=query, data=data, upsert=True)
        return dataset_rankings

    def get_site_rankings(self):
        """
        Generate site rankings
        """
        date = datetime_day(datetime.datetime.utcnow())
        rank_name = self.name + '_rank'
        popularity_name = self.name + '_popularity'
        # get all sites which can be replicated to
        site_names = self.sites.get_available_sites()
        site_rankings = dict()
        for site_name in site_names:
            # get popularity
            popularity = self.get_site_popularity(site_name)
            # get cpu and storage (performance)
            performance = self.sites.get_performance(site_name)
            # get available storage
            available_storage_tb = self.sites.get_available_storage(site_name)/10**3
            if available_storage_tb <= 0:
                available_storage_tb = 0
            #calculate rank
            try:
                rank = (performance*available_storage_tb)/popularity
            except:
                rank = 0.0
            # store into dict
            site_rankings[site_name] = rank
            # insert into database
            coll = 'site_rankings'
            query = {'name':site_name, 'date':date}
            data = {'$set':{'name':site_name, 'date':date, rank_name:rank, popularity_name:popularity}}
            self.storage.update_data(coll=coll, query=query, data=data, upsert=True)
        return site_rankings

    def get_site_popularity(self, site_name):
        """
        Get delta popularity for site
        """
        date = datetime_day(datetime.datetime.utcnow())
        rank_name = self.name + '_rank'
        # get all datasets with a replica at the site and how many replicas it has
        coll = 'dataset_data'
        pipeline = list()
        match = {'$match':{'replicas':site_name}}
        pipeline.append(match)
        project = {'$project':{'name':1, '_id':0}}
        pipeline.append(project)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        popularity = 0.0
        for dataset in data:
            dataset_name = dataset['name']
            # get the popularity of the dataset and dicide by number of replicas
            coll = 'dataset_rankings'
            pipeline = list()
            match = {'$match':{'name':dataset_name, 'date':date}}
            pipeline.append(match)
            project = {'$project':{rank_name:1, '_id':0}}
            pipeline.append(project)
            data = self.storage.get_data(coll=coll, pipeline=pipeline)
            popularity += data[0][rank_name]
        return popularity

    def get_site_storage_rankings(self, subscriptions):
        """
        Return the amount over the soft limit sites are including new subscriptions
        If site is not over just set to 0
        """
        site_rankings = dict()
        available_sites = self.sites.get_available_sites()
        for site_name in available_sites:
            site_rankings[site_name] = self.sites.get_over_soft_limit(site_name)
        for subscription in subscriptions:
            site_rankings[subscription[1]] += self.datasets.get_size(subscription[0])
        for site_name in available_sites:
            if site_rankings[site_name] < 0:
                del site_rankings[site_name]
        return site_rankings

    def normalize_popularity(self, dataset_popularity):
        """
        Normalize popularity values to be between 1 and max_replicas
        """
        dataset_rankings = dict()
        max_pop = max(dataset_popularity.iteritems(), key=operator.itemgetter(1))[1]
        min_pop = min(dataset_popularity.iteritems(), key=operator.itemgetter(1))[1]
        n = float(min_pop + (self.max_replicas - 1))/max_pop
        m = 1 - n*min_pop
        for dataset_name, popularity in dataset_popularity.items():
            # store into dict
            rank = n*dataset_popularity[dataset_name] + m
            dataset_rankings[dataset_name] = int(rank)
        return dataset_rankings
