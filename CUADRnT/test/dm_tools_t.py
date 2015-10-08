#!/usr/bin/env python2.7
"""
File       : tools_t.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Test class for tool classes
"""

# system modules
import unittest
import os

# package modules
from cuadrnt.utils.config import get_config
from cuadrnt.data_management.tools.sites import SiteManager
from cuadrnt.data_management.tools.datasets import DatasetManager

# get local config file
opt_path = os.path.join(os.path.split(os.path.dirname(os.path.realpath(__file__)))[0], 'etc')

@unittest.skip("Skip Test")
class ToolsTests(unittest.TestCase):
    """
    A test class for tools classes
    """
    def setUp(self):
        "Set up for test"
        self.config = get_config(path=opt_path, file_name='test.cfg')

    def tearDown(self):
        "Clean up"
        pass

    #@unittest.skip("Skip Test")
    def test_sites(self):
        "Test managers"
        print ""
        sites = SiteManager(config=self.config)
        sites.initiate_db()
        sites.update_cpu()
        sites.update_db()
        # test different functions

    #@unittest.skip("Skip Test")
    def test_datasets(self):
        "Test managers"
        print ""
        datasets = DatasetManager(config=self.config)
        datasets.initiate_db()
        datasets.update_db()
        # test different functions

if __name__ == '__main__':
    unittest.main()
