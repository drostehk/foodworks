#!/usr/bin/python
# -*- coding: UTF-8-*-
from __future__ import (absolute_import, division, print_function, unicode_literals)
import sys
import os

from foodworks.connector import GoogleSourceClient
from foodworks.transform import GoogleToCanonical


#gc = GoogleSourceClient.connect()
#ss = gc.open_source('TSWN','Collection',2015)


#ss = GoogleToCanonical('TSWN', 'Collection', 2015)
#ss.collection_sheets_to_csv()
#ss.donors_sheets_to_csv()

psc_list = ['PSC-Kowloon City', 'PSC-SSP', 'PSC-TM', 'PSC-Wong Tai Sin', 'PSC-YTM']
yr_list = [2014, 2015]

psc_list = ['PSC-Wong Tai Sin', 'PSC-YTM']
#yr_list = [2014, 2015]

for psc in psc_list:
    for yr in yr_list:
        print('=' * 40)
        print(psc + ' ' + str(yr))
        print('=' * 40)
        ss = GoogleToCanonical(psc, 'Collection', yr)
        ss.collection_sheets_to_csv()
        ss.donors_sheets_to_csv()
        ss.terms_sheets_to_csv()

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