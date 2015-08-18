#!/usr/bin/python
# -*- coding: UTF-8-*-
from __future__ import (absolute_import, division, print_function, unicode_literals)
import sys
import os

from foodworks.connector import GoogleSourceClient
from foodworks.transform import GoogleToCanonical


#gc = GoogleSourceClient.connect()
#ss = gc.open_source('TSWN','Collection',2015)

#ss = GoogleToCanonical('TSWN', 'Collection', 2014)
#ss.collection_sheets_to_csv()

ss = GoogleToCanonical('TSWN', 'Distribution', 2014)
ss.distribution_sheets_to_csv()
#ss.beneficiary_sheets_to_csv()

#print(ss.parse_cover_sheet())
# gc = GoogleSourceClient.connect()

## Only add but not replace when there is already a donors file
## TODO