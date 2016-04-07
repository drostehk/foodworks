#!/usr/bin/python
# -*- coding: UTF-8-*-
from __future__ import (absolute_import, division, print_function, unicode_literals)

import os
import sys
import time

sys.path.append( os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ) )

from core.connector import GoogleSourceClient
from core.transform import SheetToCanonical
from core.drive import generate_structure

for stage, ngos in generate_structure().iteritems():
    
    for ngo, programmes in ngos.iteritems():

            if not programmes:
                continue
        
            for programme, sheets in programmes.iteritems():

                print('\n>>> ', ngo, stage.capitalize(), programme, ' >>> ', len(sheets), 'Yrs')
                
                if not sheets:
                    continue
        
                for sheet in sheets:
                    ss = SheetToCanonical(**sheet)
                    
                    if stage == 'collection':
                        ss.collection_sheets_to_csv()
                        ss.donors_sheets_to_csv()
                        ss.terms_sheets_to_csv()
                    
                    elif stage == 'processing':
                        ss.finance_sheets_to_csv()
                        ss.processing_sheets_to_csv()
                    
                    elif stage == 'distribution':
                        ss.distribution_sheets_to_csv()
                        ss.beneficiary_sheets_to_csv()
