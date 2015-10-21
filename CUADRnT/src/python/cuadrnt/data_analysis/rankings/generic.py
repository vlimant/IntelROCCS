#!/usr/bin/env python2.7
"""
File       : generic.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Generic class for all ranking algorithms
"""

# system modules
import logging
import datetime

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

    def get_dataset_rankings(self):
        """
        Generate dataset rankings
        """
        dataset_names = self.datasets.get_db_datasets()
        dataset_rankings = dict()
        for dataset_name in dataset_names:
            popularity = self.get_dataset_popularity(dataset_name)
            # store into dict
            dataset_rankings[dataset_name] = popularity
        return dataset_rankings

    def get_site_rankings(self):
        """
        Generate site rankings
        """
        date = datetime_day(datetime.datetime.utcnow())
        # get all sites which can be replicated to
        site_names = self.sites.get_available_sites()
        site_rankings = dict()
        for site_name in site_names:
            # get popularity
            popularity = float(self.get_site_popularity(site_name))
            # get cpu and storage (performance)
            performance = float(self.sites.get_performance(site_name))
            # get available storage
            available_storage = float(self.sites.get_available_storage(site_name))
            if available_storage <= 0:
                available_storage = 0.0
            # insert into database
            coll = 'site_popularity'
            query = {'name':site_name, 'date':date}
            data = {'$set':{'name':site_name, 'date':date, 'delta_popularity':popularity, 'performance':performance, 'available_storage':available_storage}}
            self.storage.update_data(coll=coll, query=query, data=data, upsert=True)
            #calculate rank
            rank = (performance*available_storage)/popularity
            # store into dict
            site_rankings[site_name] = rank
            # insert into database
            coll = 'site_rankings'
            query = {'name':site_name, 'date':date}
            data = {'$set':{'name':site_name, 'date':date, 'delta_rank':rank}}
            self.storage.update_data(coll=coll, query=query, data=data, upsert=True)
        return site_rankings
