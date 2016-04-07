#!/usr/bin/python
# -*- coding: UTF-8-*-
from __future__ import (absolute_import, division, print_function, unicode_literals)

import os
import sys
import time
import json
from gspread.exceptions import HTTPError

sys.path.append( os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ) )

from core.connector import GoogleSourceClient
from core.transform import SheetToCanonical
from core.drive import generate_structure

'''
SAVE PROGRESS 
'''
progress = {}

def update_progress(progress):
     with open('progress.json', 'w') as fp:
        json.dump(progress, fp)

try:
    # Delete `structure.json` if you want to refresh the export
    with open('progress.json') as fp:
        progress = json.load(fp)

except:
    update_progress(progress)


def progress_completed(stage, ngo, programme):
    try:
        return progress[stage][ngo].has_key(programme)
        
    except KeyError:
        return False


def check_or_set(base, key):
    if not key in base:
        base[key] = {}
        update_progress(progress)

'''
/SAVE PROGRESS
'''

def export_source_sheets():
    try:
        for stage, ngos in generate_structure().iteritems():

            check_or_set(progress, stage)

            if stage in ['distribution', 'processing']:
                continue
            
            for ngo, programmes in ngos.iteritems():

                if ngo in ['FoodLink', 'NLPRA']:
                    continue

                check_or_set(progress[stage], ngo)

                if not programmes:
                    continue
            
                for programme, sheets in programmes.iteritems():

                    if progress_completed(stage, ngo, programme):
                        print('\n>>> SKIPPING >>> ', ngo, stage.capitalize(), programme, ' >>> ', len(sheets), 'Yrs')
                        continue

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

                    check_or_set(progress[stage][ngo], programme)
    
    except HTTPError:
        export_source_sheets()