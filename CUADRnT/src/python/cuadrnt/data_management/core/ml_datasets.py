#!/usr/bin/env python2.7
"""
File       : ml_datasets.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Generate dataset to use for ml classification training
"""

# system modules
import logging
import re
import sys
import getopt
import datetime
from logging.handlers import TimedRotatingFileHandler

# package modules
from cuadrnt.utils.utils import datetime_to_string
from cuadrnt.utils.utils import datetime_day
from cuadrnt.utils.utils import daterange
from cuadrnt.utils.config import get_config
from cuadrnt.utils.io_utils import export_json
from cuadrnt.data_management.tools.datasets import DatasetManager
from cuadrnt.data_management.tools.popularity import PopularityManager
from cuadrnt.data_management.core.storage import StorageManager

class MLDatasets(object):
    """
    Generate ML datasets
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.storage = StorageManager(self.config)
        self.datasets = DatasetManager(self.config)
        self.popularity = PopularityManager(self.config)
        self.opt_path = self.config['paths']['opt']
        self.data_tiers = ['GEN-SIM', 'AODSIM', 'MINIAODSIM', 'GEN-SIM-RECO', 'GEN-SIM-RAW', 'MINIAOD', 'LHE']

    def start(self):
        """
        Begin Collecting data for visualization
        """
        t1 = datetime.datetime.utcnow()
        invalid_datasets = ''
        invalid_file = open(self.opt_path + '/invalid_dataset_patterns', 'r')
        for pattern in invalid_file:
            invalid_datasets += pattern.strip() + '|'
        invalid_file.close()
        invalid_datasets = invalid_datasets[:-1]
        invalid_dataset_re = re.compile(invalid_datasets)
        dataset_names = self.datasets.get_db_datasets()
        start_date = datetime_day(datetime.datetime.utcnow() - datetime.timedelta(days=90))
        end_date = datetime_day(datetime.datetime.utcnow())
        coll = 'dataset_popularity'
        data = list()
        n_datasets = 0
        for dataset_name in dataset_names:
            if invalid_dataset_re.match(dataset_name):
                continue
            pipeline = list()
            match = {'$match':{'name':dataset_name}}
            pipeline.append(match)
            match = {'$match':{'date':{'$gte':start_date, '$lte':end_date}}}
            pipeline.append(match)
            project = {'$project': {'date':1, 'n_accesses':1, 'n_cpus':1, '_id':0}}
            pipeline.append(project)
            pop_data = self.storage.get_data(coll=coll, pipeline=pipeline)
            if not pop_data:
                continue
            dataset_data = {'dataset_name': dataset_name}
            dataset_popularity = list()
            all_dates = set()
            n_accesses = 0
            for date in daterange(start_date, end_date):
                all_dates.add(date)
            for pop_date in pop_data:
                n_accesses += pop_date['n_accesses']
                dataset_popularity.append({'date':datetime_to_string(pop_date['date']), 'n_accesses':pop_date['n_accesses'], 'n_cpus':pop_date['n_cpus']})
                all_dates.remove(pop_date['date'])
            if n_accesses < 25000:
                continue
            for empty_date in all_dates:
                dataset_popularity.append({'date':datetime_to_string(empty_date), 'n_accesses':0, 'n_cpus':0})
            dataset_popularity = sorted(dataset_popularity, key=lambda x: x['date'])
            n_datasets += 1
            self.logger.info('Inserting dataset %d', n_datasets)
            dataset_data['popularity'] = dataset_popularity
            data.append(dataset_data)
        export_json(data=data, file_name='data_visualization')
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('ML Dataset generation took %s', str(td))

def main(argv):
    """
    Main driver for ML Dataset Generation
    """
    log_level = logging.WARNING
    config = get_config(path='/var/opt/cuadrnt', file_name='cuadrnt.cfg')
    try:
        opts, args = getopt.getopt(argv, 'h', ['help', 'log='])
    except getopt.GetoptError:
        print "usage: ml_datasets.py [--log=notset|debug|info|warning|error|critical]"
        print "   or: ml_datasets.py --help"
        sys.exit()
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print "usage: ml_datasets.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: ml_datasets.py --help"
            sys.exit()
        elif opt in ('--log'):
            log_level = getattr(logging, arg.upper())
            if not isinstance(log_level, int):
                print "%s is not a valid log level" % (str(arg))
                print "usage: ml_datasets.py [--log=notset|debug|info|warning|error|critical]"
                print "   or: ml_datasets.py --help"
                sys.exit()
        else:
            print "usage: ml_datasets.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: ml_datasets.py --help"
            print "error: option %s not recognized" % (str(opt))
            sys.exit()

    log_path = config['paths']['log']
    log_file = 'ml_datasets.log'
    file_name = '%s/%s' % (log_path, log_file)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    handler = TimedRotatingFileHandler(file_name, when='midnight', interval=1, backupCount=1)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    ml_datasets = MLDatasets(config)
    ml_datasets.start()

if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()
