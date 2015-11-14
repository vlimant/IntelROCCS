#!/usr/bin/env python2.7
"""
File       : preprocess.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Remove datasets which have less than 10 accesses in the last 30 days
"""

# system modules
import logging
import sys
import json
import getopt
import datetime
from math import log
from logging.handlers import TimedRotatingFileHandler

# package modules
from cuadrnt.utils.config import get_config
from cuadrnt.utils.utils import daterange
from cuadrnt.utils.io_utils import export_json
from cuadrnt.data_management.tools.datasets import DatasetManager
from cuadrnt.data_management.tools.popularity import PopularityManager
from cuadrnt.data_management.core.storage import StorageManager

class Preprocess(object):
    """
    preprocess ml data based on classifications made in vis tool
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.datasets = DatasetManager(self.config)
        self.popularity = PopularityManager(self.config)
        self.storage = StorageManager(self.config)
        self.data_path = self.config['paths']['data']
        self.data_tiers = config['tools']['valid_tiers'].split(',')

    def start(self):
        """
        Begin preprocess ml data
        """
        t1 = datetime.datetime.utcnow()
        fd = open(self.data_path + '/classifications.json', 'r')
        classifications = json.load(fd)
        fd.close()
        for data_tier in self.data_tiers:
            X = list()
            Y_trend = list()
            Y_avg = list()
            for classification in classifications:
                dataset_name = classification['dataset_name']
                dataset_features = self.datasets.get_dataset_features(dataset_name)
                if not (dataset_features['data_tier'] == data_tier):
                    continue
                date = datetime.datetime.strptime(classification['date'].split('T')[0], '%Y-%m-%d')
                class_str = classification['classification']
                y_trend = self.get_y_trend(class_str)
                y_avg = self.get_y_avg(dataset_name, date)
                x = self.get_x(dataset_name, date)
                Y_trend.append(y_trend)
                Y_avg.append(y_avg)
                X.append(x)
            training_data = {'features':X, 'trend_classifications':Y_trend, 'avg_classifications':Y_avg}
            file_name = 'training_data_' + data_tier
            export_json(data=training_data, file_name=file_name)
            self.logger.info('There are a total of %d training sets for data tier %s', len(training_data['trend_classifications']), data_tier)
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('Preprocess data took %s', str(td))

    def get_x(self, dataset_name, start_date):
        """
        Get popularity data for 7 days beginning at date for dataset
        """
        x = list()
        end_date = start_date + datetime.timedelta(days=7)
        for date in daterange(start_date, end_date):
            coll = 'dataset_popularity'
            pipeline = list()
            match = {'$match':{'name':dataset_name}}
            pipeline.append(match)
            match = {'$match':{'date':date}}
            pipeline.append(match)
            project = {'$project':{'n_accesses':1, 'n_cpus':1, '_id':0}}
            pipeline.append(project)
            data = self.storage.get_data(coll=coll, pipeline=pipeline)
            try:
                x.append(log(int(data[0]['n_accesses']))*log(int(data[0]['n_cpus'])))
            except:
                x.append(0.0)
        return x

    def get_y_trend(self, class_str):
        """
        Get classification from string
        """
        if class_str == 'unchanged':
            y = 0
        elif class_str == 'increasing':
            y = 1
        elif class_str == 'decreasing':
            y = -1
        else:
            y = 0
        return y

    def get_y_avg(self, dataset_name, start_date):
        """
        Get average popularity for 7 days beginning at date+7 days for dataset
        popularity = n_accesses*n_cpus
        """
        start_date = start_date + datetime.timedelta(days=6)
        end_date = start_date + datetime.timedelta(days=7)
        coll = 'dataset_popularity'
        pipeline = list()
        match = {'$match':{'name':dataset_name}}
        pipeline.append(match)
        match = {'$match':{'date':{'$gte':start_date, '$lte':end_date}}}
        pipeline.append(match)
        group = {'$group':{'_id':'$name', 'avg_popularity':{'$avg': {'$multiply':['$n_accesses', '$n_cpus']}}}}
        pipeline.append(group)
        data = self.storage.get_data(coll=coll, pipeline=pipeline)
        try:
            y = log(int(data[0]['avg_popularity']))
        except:
            y = 0.0
        return y

def main(argv):
    """
    Main driver for preprocess ml data
    """
    log_level = logging.WARNING
    config = get_config(path='/var/opt/cuadrnt', file_name='cuadrnt.cfg')
    try:
        opts, args = getopt.getopt(argv, 'h', ['help', 'log='])
    except getopt.GetoptError:
        print "usage: preprocess.py [--log=notset|debug|info|warning|error|critical]"
        print "   or: preprocess.py --help"
        sys.exit()
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print "usage: preprocess.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: preprocess.py --help"
            sys.exit()
        elif opt in ('--log'):
            log_level = getattr(logging, arg.upper())
            if not isinstance(log_level, int):
                print "%s is not a valid log level" % (str(arg))
                print "usage: preprocess.py [--log=notset|debug|info|warning|error|critical]"
                print "   or: preprocess.py --help"
                sys.exit()
        else:
            print "usage: preprocess.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: preprocess.py --help"
            print "error: option %s not recognized" % (str(opt))
            sys.exit()

    log_path = config['paths']['log']
    log_file = 'preprocess.log'
    file_name = '%s/%s' % (log_path, log_file)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    handler = TimedRotatingFileHandler(file_name, when='midnight', interval=1, backupCount=1)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    preprocess = Preprocess(config)
    preprocess.start()

if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()
