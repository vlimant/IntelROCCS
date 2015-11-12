#!/usr/bin/env python2.7
"""
File       : svm.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: SVM ranking algorithm
"""

# system modules
import logging
import datetime
import json
import math
import numpy as np
from sklearn.svm import SVC
from sklearn.externals import joblib

# package modules
# from cuadrnt.utils.utils import datetime_day
from cuadrnt.data_analysis.rankings.generic import GenericRanking

class SVMRanking(GenericRanking):
    """
    Use Support Vector Machines to rank datasets and sites
    Subclass of GenericRanking
    """
    def __init__(self, config=dict()):
        GenericRanking.__init__(self, config)
        self.logger = logging.getLogger(__name__)
        self.name = 'svm'
        self.data_path = self.config['paths']['data']
        self.data_tiers = config['tools']['valid_tiers'].split(',')
        try:
            self.clf = joblib.load(self.data_path + 'svm.pkl')
        except:
            self.logger.info('SVM classifier need to be trained')
            self.clf = SVC(kernel='poly', probability=True, C=0.5)
        self.preprocessed_data = dict()

    def get_dataset_popularity(self, dataset_name):
        """
        Get expected popularity for dataset
        """

    def get_dataset_trend(self, dataset_name):
        """
        Classify dataset as either increasing, decreasing or unchanged popularity
        """

    def train(self):
        """
        Training classifier
        """
        fd = open(self.data_path + '/training_data.json', 'r')
        self.preprocessed_data = json.load(fd)
        fd.close()
        tot = len(self.preprocessed_data['classifications'])
        p = int(math.ceil(tot*0.8))
        training_features = np.array(self.preprocessed_data['features'][:p])
        training_classifications = np.array(self.preprocessed_data['classifications'][:p])
        t1 = datetime.datetime.utcnow()
        self.clf.fit(training_features, training_classifications)
        t2 = datetime.datetime.utcnow()
        td = t2 - t1
        self.logger.info('Training SVM took %s', str(td))
        #joblib.dump(self.clf, 'svm.pkl')

    def test(self):
        """
        Test accuracy of classifier
        """
        tot = len(self.preprocessed_data['classifications'])
        p = int(math.floor(tot*0.2))
        test_features = np.array(self.preprocessed_data['features'][p:])
        test_classifications = np.array(self.preprocessed_data['classifications'][p:])
        accuracy = self.clf.score(test_features, test_classifications)
        self.logger.info('The accuracy of SVM is %.3f', accuracy)
