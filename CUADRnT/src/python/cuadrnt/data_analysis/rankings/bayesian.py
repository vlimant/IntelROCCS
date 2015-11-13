#!/usr/bin/env python2.7
"""
File       : bayesian.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Bayesian based ranking algorithm
"""

# system modules
import logging
import datetime
import json
import math
import numpy as np
from sklearn.naive_bayes import GaussianNB
from sklearn.linear_model import BayesianRidge
from sklearn.externals import joblib

# package modules
from cuadrnt.data_analysis.rankings.generic import GenericRanking

class BayesianRanking(GenericRanking):
    """
    Use Naive Bayes and Bayesian Ridge to rank datasets and sites
    Subclass of GenericRanking
    """
    def __init__(self, config=dict()):
        GenericRanking.__init__(self, config)
        self.logger = logging.getLogger(__name__)
        self.name = 'bayesian'
        self.data_path = self.config['paths']['data']
        self.data_tiers = config['tools']['valid_tiers'].split(',')
        self.preprocessed_data = dict()
        self.clf_trend = dict()
        self.clf_avg = dict()
        for data_tier in self.data_tiers:
            try:
                self.clf_trend[data_tier] = joblib.load(self.data_path + 'bayesian_trend_' + data_tier + '.pkl')
                self.clf_avg[data_tier] = joblib.load(self.data_path + 'bayesian_avg_' + data_tier + '.pkl')
            except:
                self.logger.info('Bayesian classifier and regressor for data tier %s need to be trained', data_tier)
                self.clf_trend[data_tier] = GaussianNB()
                self.clf_avg[data_tier] = BayesianRidge()

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
        Training classifier and regressor
        """
        for data_tier in self.data_tiers:
            fd = open(self.data_path + '/training_data_' + data_tier + '.json', 'r')
            self.preprocessed_data[data_tier] = json.load(fd)
            fd.close()
            tot = len(self.preprocessed_data[data_tier]['trend_features'])
            p = int(math.ceil(tot*0.8))
            trend_training_features = np.array(self.preprocessed_data[data_tier]['trend_features'][:p])
            avg_training_features = np.array(self.preprocessed_data[data_tier]['trend_features'][:p])
            trend_training_classifications = np.array(self.preprocessed_data[data_tier]['trend_classifications'][:p])
            avg_training_classifications = np.array(self.preprocessed_data[data_tier]['avg_classifications'][:p])
            t1 = datetime.datetime.utcnow()
            self.clf_trend[data_tier].fit(trend_training_features, trend_training_classifications)
            self.clf_avg[data_tier].fit(avg_training_features, avg_training_classifications)
            t2 = datetime.datetime.utcnow()
            td = t2 - t1
            self.logger.info('Training Bayesian models for data tier %s took %s', data_tier, str(td))
            #joblib.dump(self.clf_trend[data_tier], self.data_path + 'bayesian_trend_' + data_tier + '.pkl')
            #joblib.dump(self.clf_avg[data_tier], self.data_path + 'bayesian_avg_' + data_tier + '.pkl')

    def test(self):
        """
        Test accuracy/score of classifier and regressor
        """
        for data_tier in self.data_tiers:
            tot = len(self.preprocessed_data[data_tier]['trend_features'])
            p = int(math.floor(tot*0.2))
            trend_test_features = np.array(self.preprocessed_data[data_tier]['trend_features'][p:])
            avg_test_features = np.array(self.preprocessed_data[data_tier]['trend_features'][p:])
            trend_test_classifications = np.array(self.preprocessed_data[data_tier]['trend_classifications'][p:])
            avg_test_classifications = np.array(self.preprocessed_data[data_tier]['avg_classifications'][p:])
            accuracy_trend = self.clf_trend[data_tier].score(trend_test_features, trend_test_classifications)
            accuracy_avg = self.clf_avg[data_tier].score(avg_test_features, avg_test_classifications)
            self.logger.info('The accuracy of Naive Bayes trend classifier for data tier %s is %.3f', data_tier, accuracy_trend)
            self.logger.info('The accuracy of Bayesian Ridge avg regressor for data tier %s is %.3f', data_tier, accuracy_avg)
