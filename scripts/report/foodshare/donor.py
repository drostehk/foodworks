#!/usr/bin/python
# -*- coding: UTF-8-*-
from __future__ import (absolute_import, division, print_function, unicode_literals)

import os
import sys
import pdb
import numpy as np
import pandas as pd
import operator as op

import warnings
warnings.filterwarnings("error")

from datetime import datetime, timedelta, date, time

# Ugly Hack
sys.path.append( os.path.dirname(os.path.dirname(
    os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ) ) ) )

from core.drive import generate_structure

class ECFReport(object):
    """docstring for ECFReport

    Example:

    ecf = ECFReport()
    ecf.generate_all_reports()

    The only public methods are :

        .generate_all_reports()
        .generate_single_reports(ngo,stage,progammes)

    """
    def __init__(self, **kwargs):
        super(ECFReport, self).__init__()

        self.ROOT_FOLDER = 'data/Canonical/'
        self.REPORT_FOLDER = 'data/Report/'
        
        self.STAGES = ['collection','distribution']
        self.STAGE_TITLE = {
            'collection': 'Recovery',
            'distribution': 'Distribution'
        }

        self.SORT_KEY = []
        self.MELT_INDEX = ['datetime', 'donor', 'programme']
        self.FINANCE_INDEX = ['month', 'income', 'expenditure']
        self.REPORT_INDEX = ['datetime', 'donor', 'organisation_id', 'programme', 'variable',
                        'value', 'canonical', 'donor_category', 'year', 'month', 'day']

        self.META_FILES_PROGRAMME = []
        self.META_FILES_NGO = ['donors']
        self.META_FILES = ['map']

        self.PERIOD = (datetime.now() - timedelta(days=28))
        self.MONTH_NUM = self.PERIOD.month
        self.MONTH_NAME = self.PERIOD.strftime('%B')
        self.YEAR_NUM = self.PERIOD.year

        # WHITE & BLACKLISTING

        # SKIP_NGO = ['NLPRA']
        self.SKIP_NGO = []
        self.ONLY_NGO = []

        # SKIP_STAGES = ['collection']
        self.SKIP_STAGES = []
        self.ONLY_STAGES = []

        self.SKIP_PROGRAMMES = [u'Amenities']
        self.ONLY_PROGRAMMES = None
        # self.ONLY_PROGRAMMES = ['ECF Van 01','ECF Van 02','ECF Van 03','General']

        # Override the defaults
        if kwargs is not None:
            for k, v in kwargs.iteritems():
                setattr(self, k, v)
        
        self.ngo = ''
        self.stage = ''
        self.programmes = []

        self.meal_weight = 0.585
    
        print('GENERATING REPORT FOR PERIOD\n ', self.YEAR_NUM, '-', self.MONTH_NUM)

    def generate_all_reports(self, year=datetime.now().year, iteration=1):

        print('\n### LOADING DRIVE STRUCTURE### {}'.format(iteration))
        
        for stage, ngos in generate_structure().iteritems():

            print('\n### ITERATION ### {}'.format(iteration))
            # Selective processing of stages
            if stage in self.SKIP_STAGES:
                continue

            # Exclusive processing of stages
            if self.ONLY_STAGES and not stage in self.ONLY_STAGES:
                continue

            self.stage = stage
            
            for ngo, programmes in ngos.iteritems():

                # Selective processing of NGOS
                if ngo in self.SKIP_NGO:
                    continue

                # Exclusive processing of NGOS
                if self.ONLY_NGO and not ngo in self.ONLY_NGO:
                    print("### SKIP {} ###".format(ngo.upper()))
                    continue

                self.ngo = ngo

                if not programmes:
                    continue

                programmes = [p for p in programmes.keys()]

                self.programmes = programmes
                           
                self.generate_report()

    
    def generate_single_report(self, ngo, stage, programmes):
        self.ngo = ngo
        self.stage = stage
        self.programmes = programmes
        self.generate_report()        
    
    
    # PRIVATE METHODS 