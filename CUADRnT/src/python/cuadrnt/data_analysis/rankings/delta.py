#!/usr/bin/env python2.7
"""
File       : delta.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Delta ranking algorithm
"""

# system modules
import logging
import datetime
from math import log

# package modules
from cuadrnt.utils.utils import datetime_day
from cuadrnt.data_analysis.rankings.generic import GenericRanking

class DeltaRanking(GenericRanking):
    """
    Use delta popularity values to rank datasets and sites
    Subclass of GenericRanking
    """
    def __init__(self, config=dict()):
        GenericRanking.__init__(self, config)
        self.logger = logging.getLogger(__name__)
        self.name = 'delta'

    def get_dataset_popularity(self, dataset_name):
        """
        Get delta popularity for dataset
        """
        size_gb = self.datasets.get_size(dataset_name)
        coll = 'dataset_popularity'
        start_date = datetime_day(datetime.datetime.utcnow()) - datetime.timedelta(days=14)
        end_date = datetime_day(datetime.datetime.utcnow()) - datetime.timedelta(days=8)
        pipeline = list()
        match = {'$match':{'name':dataset_name}}
        pipeline.append(match)
        match = {'$match':{'date':{'$gte':start_date, '$lte':end_date}}}
        pipeline.append(match)
        group = {'$group':{'_id':'$name', 'old_popularity':{'$sum':{'$multiply':['$n_accesses', '$n_cpus']}}}}
        pipeline.append(group)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        try:
            old_pop = float(data[0]['old_popularity'])
        except:
            old_pop = 0
        start_date = datetime_day(datetime.datetime.utcnow()) - datetime.timedelta(days=7)
        end_date = datetime_day(datetime.datetime.utcnow()) - datetime.timedelta(days=1)
        pipeline = list()
        match = {'$match':{'name':dataset_name}}
        pipeline.append(match)
        match = {'$match':{'date':{'$gte':start_date, '$lte':end_date}}}
        pipeline.append(match)
        group = {'$group':{'_id':'$name', 'new_popularity':{'$sum': {'$multiply':['$n_accesses', '$n_cpus']}}}}
        pipeline.append(group)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        try:
            new_pop = float(data[0]['new_popularity'])
        except:
            new_pop = 0
        delta_popularity = (new_pop - old_pop)/size_gb
        if delta_popularity >= 1:
            delta_popularity = log(delta_popularity)
        else:
            delta_popularity = 0
        return delta_popularity
