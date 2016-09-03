#!/usr/bin/python
# -*- coding: UTF-8-*-
from __future__ import (absolute_import, division, print_function, unicode_literals)

import os
import sys
import time
import json
from gspread.exceptions import HTTPError

# TODO FEATURES 

# Figure out why the beneficies export for the FoodLink General programme only had one line - was it an export error or something worse? - #42

sys.path.append( os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ) )

from core.transform import SheetToCanonical
from core.drive import generate_structure
from scripts import print_error, print_warning

'''
SAVE PROGRESS 
'''
progress = {}

def update_progress(progress):
     with open('progress.json', 'w') as fp:
        json.dump(progress, fp, indent=4, sort_keys=True)

try:
    # Delete `structure.json` if you want to refresh the export
    with open('progress.json') as fp:
        progress = json.load(fp)

except:
    update_progress(progress)


def progress_check(stage, ngo, programme):
    try:
        return progress[stage][ngo].has_key(programme)
        
    except KeyError:
        return False


def check_or_set(base, key, status=None):

    if status is not None:
        base[key] = status

    elif key not in base:
        base[key] = {}
    
    update_progress(progress)

'''
/SAVE PROGRESS
'''


def iterate_over_sheets(stage, ngo, programme, sheets, iteration, skip_progress_check, year=None):
    
    if not skip_progress_check and progress_check(stage, ngo, programme):
        if progress[stage][ngo][programme]:
            print('\n>>> COMPLETED >>> ', ngo, stage.capitalize(), programme, ' >>> ', len(sheets), 'Yrs')
        else:
            print('\n>>> MUCH FAIL >>> ', ngo, stage.capitalize(), programme, ' >>> ', len(sheets), 'Yrs')
        return 

    print('\n>>> ', ngo, stage.capitalize(), programme, ' >>> ', len(sheets), 'Yrs')
    
    if not sheets:
        return

    for sheet in sheets:
        # Only export current year if needed
        if year and str(year) not in sheet['name']:
            continue

        ss = SheetToCanonical(**sheet)
        
        if stage == 'collection':
            ss.collection_sheets_to_csv()
            try:
                ss.donors_sheets_to_csv()
            except ValueError as e:
                if str(e) == 'Plan shapes are not aligned':
                    import webbrowser
                    url_base = "https://docs.google.com/spreadsheets/d/"
                    webbrowser.open_new(url_base + sheet['id'])
                    print_error('INVALID DATA STRUCTURE', ['N/A'])
                    sys.exit()

        
        elif stage == 'processing':
            ss.finance_sheets_to_csv()
            ss.processing_sheets_to_csv()
        
        elif stage == 'distribution':
            ss.distribution_sheets_to_csv()
            ss.beneficiary_sheets_to_csv()

    if not skip_progress_check:
        check_or_set(progress[stage][ngo], programme, True)

def export_source_sheets(iteration=1, skip_progress_check=False, developer_mode=False, **kwargs):

    SKIP_NGO = ['NLPRA']
    ONLY_NGO = []
    SKIP_STAGES = []
    ONLY_STAGES = ['collection', 'distribution', 'processing']
    YEAR = None

    print('\n### LOADING DRIVE STRUCTURE### {}'.format(iteration))

    if kwargs is not None:
        for k, v in kwargs.iteritems():
            if k == 'ngo':
                ONLY_NGO = [v]
            if k == 'skip':
                SKIP_NGO = SKIP_NGO + v
            if k == 'skipstages':
                SKIP_STAGES = SKIP_STAGES + v
            if k == 'year':
                YEAR = SKIP_STAGES + v

    for stage, ngos in generate_structure().iteritems():

        check_or_set(progress, stage)

        print('\n### ITERATION ### {}'.format(iteration))
        # Selective processing of stages
        if stage in SKIP_STAGES:
            continue

        # Exclusive processing of stages
        if ONLY_STAGES and not stage in ONLY_STAGES:
            continue
        
        for specific_ngo, programmes in ngos.iteritems():

            # Selective processing of NGOS
            if specific_ngo in SKIP_NGO:
                continue

            # Exclusive processing of NGOS
            if ONLY_NGO and not specific_ngo in ONLY_NGO:
                continue

            check_or_set(progress[stage], specific_ngo)

            if not programmes:
                continue
        
            for programme, sheets in programmes.iteritems():

                if developer_mode:
                    iterate_over_sheets(stage, specific_ngo, programme, sheets, iteration, skip_progress_check, YEAR)

                else:
                    retry_export_on_failed_attempt(iterate_over_sheets, stage, specific_ngo, programme, sheets, iteration, skip_progress_check, YEAR, **kwargs)


def retry_export_on_failed_attempt(fn, stage, specific_ngo, programme, sheets, iteration, skip_progress_check, year, **kwargs):
    try:
        fn(stage, specific_ngo, programme, sheets, iteration, skip_progress_check, year)
    except HTTPError as e:
        print(e)
        export_source_sheets(iteration+1, skip_progress_check,**kwargs)

    # except Exception as e:
    #     print(e)
    #     if not skip_progress_check:
    #         check_or_set(progress[stage][ngo], programme, False)
    #         export_source_sheets(iteration+1, skip_progress_check)
    #     else:
    #         import pdb; pdb.set_trace()


    # Refector the Terms
    # ss.terms_sheets_to_csv()

if __name__ == '__main__':
    export_source_sheets()
