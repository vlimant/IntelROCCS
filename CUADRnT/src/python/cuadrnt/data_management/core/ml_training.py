#!/usr/bin/env python2.7
"""
File       : ml_training.py
Author     : Train ML classifier
"""

# system modules
import logging
import sys
import getopt
import datetime
from logging.handlers import TimedRotatingFileHandler

# package modules
from cuadrnt.utils.config import get_config
from cuadrnt.data_analysis.rankings.svm import SVMRanking
from cuadrnt.data_analysis.rankings.bayesian import BayesianRanking

class MLTraining(object):
    """
    Generate ML datasets
    """
    def __init__(self, config=dict()):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.svm_rankings = SVMRanking(self.config)
        self.bayesian_rankings = BayesianRanking(self.config)

    def start(self):
        """
        Begin Collecting data for visualization
        """
        t1 = datetime.datetime.utcnow()
        self.svm_rankings.train()
        self.svm_rankings.test()
        self.bayesian_rankings.train()
        self.bayesian_rankings.test()
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('ML Training took %s', str(td))

def main(argv):
    """
    Main driver for ML Trainer
    """
    log_level = logging.WARNING
    config = get_config(path='/var/opt/cuadrnt', file_name='cuadrnt.cfg')
    try:
        opts, args = getopt.getopt(argv, 'h', ['help', 'log='])
    except getopt.GetoptError:
        print "usage: ml_training.py [--log=notset|debug|info|warning|error|critical]"
        print "   or: ml_training.py --help"
        sys.exit()
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print "usage: ml_training.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: ml_training.py --help"
            sys.exit()
        elif opt in ('--log'):
            log_level = getattr(logging, arg.upper())
            if not isinstance(log_level, int):
                print "%s is not a valid log level" % (str(arg))
                print "usage: ml_training.py [--log=notset|debug|info|warning|error|critical]"
                print "   or: ml_training.py --help"
                sys.exit()
        else:
            print "usage: ml_training.py [--log=notset|debug|info|warning|error|critical]"
            print "   or: ml_training.py --help"
            print "error: option %s not recognized" % (str(opt))
            sys.exit()

    log_path = config['paths']['log']
    log_file = 'ml_training.log'
    file_name = '%s/%s' % (log_path, log_file)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    handler = TimedRotatingFileHandler(file_name, when='midnight', interval=1, backupCount=1)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d: %(message)s', datefmt='%H:%M')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    ml_training = MLTraining(config)
    ml_training.start()

if __name__ == "__main__":
    main(sys.argv[1:])
    sys.exit()
