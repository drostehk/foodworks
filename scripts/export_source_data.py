#!/usr/bin/python
# -*- coding: UTF-8-*-
from __future__ import (absolute_import, division, print_function, unicode_literals)
import os
import sys

from foodworks.core.connector import GoogleSourceClient
from foodworks.core.transform import GoogleToCanonical
import time


#gc = GoogleSourceClient.connect()
#ss = gc.open_source('TSWN','Collection',2015)


#ss = GoogleToCanonical('TSWN', 'Collection', 2015)
#ss.collection_sheets_to_csv()
#ss.donors_sheets_to_csv()

psc_list = ['PSC-Kowloon City', 'PSC-SSP', 'PSC-TM', 'PSC-Wong Tai Sin', 'PSC-YTM']
yr_list = [2014, 2015]

psc_list = ['PSC-Wong Tai Sin', 'PSC-YTM']
#yr_list = [2014, 2015]

psc_list = ['TSWN']

psc_list = ['PSC-YTM']
'''
for psc in psc_list:
    for yr in yr_list:
        print('=' * 40)
        print(psc + ' ' + str(yr))
        print('=' * 40)
        ss = GoogleToCanonical(psc, 'Collection', yr)
        #ss.collection_sheets_to_csv()
        #ss.donors_sheets_to_csv()
        time.sleep(10)
        ss.terms_sheets_to_csv()
'''

ngo_list = ['PSC-Kowloon City', 'PSC-SSP', 'PSC-TM', 'PSC-Wong Tai Sin', 'PSC-YTM', 'PCSS', 'SWA', 'TSWN', 'WSA']

ngo_list = ['TSWN']
ngo_list = ['PSC-Kowloon City']
ngo_list = ['SWA']
ngo_list = ['PSC-Wong Tai Sin']

ngo_list = ['PSC-SSP', 'PSC-TM', 'PSC-YTM', 'PCSS', 'WSA']
ngo_list = ['PCSS']
#ngo_list = ['PSC-Kowloon City', 'PSC-SSP', 'PSC-TM', 'PSC-Wong Tai Sin', 'PSC-YTM']
#ngo_list = ['PCSS']
#ngo_list = ['WSA']
'''
for ngo in ngo_list:
    print('=' * 40)
    print(ngo)
    print('=' * 40)
    print(('>' * 5) + 'Collection')
    ss = GoogleToCanonical(ngo, 'Collection', 2015)
    ss.collection_sheets_to_csv()
    ss.donors_sheets_to_csv()
    ss.terms_sheets_to_csv()
    print(('>' * 5) + 'Processing')
    ss = GoogleToCanonical(ngo, 'Processing', 2015)
    ss.finance_sheets_to_csv()
    ss.processing_sheets_to_csv()
    print(('>' * 5) + 'Distribution')
    ss = GoogleToCanonical(ngo, 'Distribution', 2015)
    ss.distribution_sheets_to_csv()
    ss.beneficiary_sheets_to_csv()
'''
ngo_dict = {"WSA": {"collection": [2015], "distribution": [2015], "processing": [2015]},
            "TSWN": {"collection": [2013, 2014, 2015], "distribution": [2015], "processing": [2015]},
            "Evergreen": {"collection": [2015], "distribution": [], "processing": []},
            "SWA": {"collection": [2015], "distribution": [2015], "processing": [2015]},
            "Action Health": {"collection": [], "distribution": [], "processing": []},
            "PCSS": {"collection": [2015], "distribution": [2015], "processing": [2015]},
            "PSC-SSP": {"collection": [2014, 2015], "distribution": [], "processing": []},
            "PSC-Kowloon City": {"collection": [2014, 2015], "distribution": [2015], "processing": [2015]},
            "PSC-TM": {"collection": [2014, 2015], "distribution": [2015], "processing": [2015]},
            "PSC-YTM": {"collection": [2014, 2015], "distribution": [2015], "processing": [2015]},
            "PSC-Wong Tai Sin": {"collection": [2014, 2015], "distribution": [2015], "processing": [2015]}
           }

ngo_dict = {
            "WSA": {"collection": [2015], "distribution": [2015], "processing": [2015]},
            "Evergreen": {"collection": [2015], "distribution": [2015], "processing": [2015]},
           }

for ngo in ngo_dict:
    print('=' * 40)
    print(ngo)
    print('=' * 40)

    if len(ngo_dict[ngo]["collection"]) > 0:
        for yr in ngo_dict[ngo]["collection"]:
            print(('>' * 5) + 'Collection - ' + str(yr))
            ss = GoogleToCanonical(ngo, 'Collection', yr)
            ss.collection_sheets_to_csv()
            ss.donors_sheets_to_csv()
            ss.terms_sheets_to_csv()

    if len(ngo_dict[ngo]["processing"]) > 0:
        for yr in ngo_dict[ngo]["processing"]:
            print(('>' * 5) + 'Processing - ' + str(yr))
            ss = GoogleToCanonical(ngo, 'Processing', yr)
            ss.finance_sheets_to_csv()
            ss.processing_sheets_to_csv()

    if len(ngo_dict[ngo]["distribution"]) > 0:
        for yr in ngo_dict[ngo]["distribution"]:
            print(('>' * 5) + 'Distribution - ' + str(yr))
            ss = GoogleToCanonical(ngo, 'Distribution', yr)
            ss.distribution_sheets_to_csv()
            ss.beneficiary_sheets_to_csv()




#ss = GoogleToCanonical('PSC-Kowloon City', 'Collection', 2015)
#ss.collection_sheets_to_csv()
#ss.donors_sheets_to_csv()
#ss.terms_sheets_to_csv()

#==Done==
'''
ss = GoogleToCanonical('TSWN', 'Processing', 2015)
ss.finance_sheets_to_csv()
ss.processing_sheets_to_csv()
'''
#==Done==
'''
ss = GoogleToCanonical('TSWN', 'Distribution', 2015)
ss.distribution_sheets_to_csv()
ss.beneficiary_sheets_to_csv()
'''
#print(ss.parse_cover_sheet())
# gc = GoogleSourceClient.connect()

## Only add but not replace when there is already a donors file
## TODO