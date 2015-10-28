#!/usr/bin/env python2.7
"""
File       : ml_datasets.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Generate dataset to use for ml classification training
"""

# system modules
import logging
import sys
import getopt
import datetime
from logging.handlers import TimedRotatingFileHandler

# package modules
from cuadrnt.utils.config import get_config
from cuadrnt.utils.io_utils import export_json
from cuadrnt.data_management.tools.datasets import DatasetManager
from cuadrnt.data_management.tools.popularity import PopularityManager

class MLDatasets(object):
    """
    Generate ML datasets
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.datasets = DatasetManager(self.config)
        self.popularity = PopularityManager(self.config)

    def start(self):
        """
        Begin Initiating Database
        """
        t1 = datetime.datetime.utcnow()
        dataset_names = self.datasets.get_db_datasets()
        data = list()
        for dataset_name in dataset_names:
            dataset_data = self.datasets.get_dataset_data(dataset_name)
            dataset_popularity = self.popularity.get_dataset_popularity(dataset_name)
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
