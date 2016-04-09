#!/usr/bin/python
# -*- coding: UTF-8-*-
from __future__ import (absolute_import, division, print_function, unicode_literals)

import os
import sys
import numpy as np
import pandas as pd
import operator as op

from datetime import datetime, timedelta, date, time

# Ugly Hack
sys.path.append( os.path.dirname(os.path.dirname(
    os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ) ) ) )

from core.drive import generate_structure

ROOT_FOLDER = 'data/Canonical/'
FRESH_FOOD_CATEGORIES = ["Vegetable", "Leafy Veg", "Ground Veg", 
                         "Soy Products", "Fruit", "Bread", "Meat",
                         "Seafood", "Cooked Food", "Fresh Other"]
PACKAGED_FOOD_CATEGORIES = ["Staple", "Frozen", "Condiments", 
                            "Drinks", "Milk Powder", "Packaged Other"]

STAGES = ['collection','processing','distribution']
META_FILES_PROGRAMME = ['beneficiary','finance']
META_FILES_NGO = ['donors']
META_FILES = ['units','i18n','map']

def get_report_dates():
    CURRENT_YEAR = datetime.now().year
    REPORT_PERIOD_STARTDATES = [datetime.strptime(d, "%d/%m").replace(year=CURRENT_YEAR).date() for d in ['1/4','1/9','1/12']]
    REPORT_PERIOD_ENDDATES   = [datetime.strptime(d, "%d/%m").replace(year=CURRENT_YEAR).date() for d in ['31/8','30/11','31/3']]

    # Silly Hack
    REPORT_PERIOD_STARTDATES[-1] = REPORT_PERIOD_STARTDATES[-1].replace(year=CURRENT_YEAR-1)

    min_index = lambda values: min(xrange(len(values)), key=values.__getitem__)
    period_index  = min_index([d - datetime.now().date() for d in REPORT_PERIOD_ENDDATES])

    return map(lambda x : x[period_index], [REPORT_PERIOD_STARTDATES, REPORT_PERIOD_ENDDATES])

REPORT_STARTDATE, REPORT_ENDDATE = get_report_dates()

print(REPORT_STARTDATE, REPORT_ENDDATE)

def generate_all_foodshare_reports(year=datetime.now().year):

    for stage, ngos in generate_structure().iteritems():

        for ngo, programmes in ngos.iteritems():

            # Selective processing of NGOS
            if ngo in ['FoodLink', 'NLPRA']:
                continue

            for programme, sheets in programmes.iteritems():

                generate_foodshare_report(ngo, programme)


def generate_foodshare_report(ngo, programme):
    fns = available_csvs(ngo)
    df_map = map_csv_to_dataframe(ngo, fns, programme)

    df_map['collection'] = clean_df(df_map, 'collection')

    import pdb
    pdb.set_trace()


# Data Processing 

def map_csv_to_dataframe(ngo, fns, programme):

    fns_in_programme = filter(lambda fn: programme in fn, fns)

    df_map = {}
        
    for stage in STAGES + META_FILES_PROGRAMME + META_FILES_NGO:
        fns = filter(lambda fn: stage in fn, fns_in_programme)
        path = ROOT_FOLDER + '/' + ngo + '/'
        df_map[stage] = pd.concat([pd.read_csv(path + fn) for fn in fns])

    return df_map


def clean_df(df_map, key):
    df = df_map[key]
    df = df_slice_report_period(df,'datetime')
    df = df.fillna(0)
   
    return df

def df_slice_report_period(df, dt_key):
    df[dt_key] = pd.to_datetime(df[dt_key])

    start = df[dt_key].searchsorted(datetime.combine(REPORT_STARTDATE, time()))[0]
    end = df[dt_key].searchsorted(datetime.combine(REPORT_ENDDATE, time()))[0]
    
    return df.iloc[start:end]


# Utilities 

def available_csvs(ngo):
    return os.listdir(ROOT_FOLDER + ngo)

def check_fresh(element):
    return element.canonical in FRESH_FOOD_CATEGORIES

def getYear(element):
    return element.datetime.year

def getMonth(element):
    return element.datetime.month

def getDay(element):
    return element.datetime.day

def find(the_series, the_value):
    return (''.join(map(str,[the_series for the_series, x in enumerate(the_series) if x == the_value])))

def fixAllmth(the_series):
    the_list = []
    check_list = the_series.index.tolist()
    for mth in range(1, 13):
        if(find(check_list, mth).isdigit()):
            the_list = the_list + [the_series[mth]]
        else:
            the_list = the_list + [0]
    return the_list

def getList(the_df, target_element):
    return the_df[the_df.element == target_element].ix[: , 1:].values[0]

def genRow(the_name, the_series):
    the_series = fixAllmth(the_series)
    return [the_name] + the_series

def getMonthNum(element):
    return ([i for i, x in enumerate(['Jan', 'Feb', 'Mar', 'Apr', 'May',
                            'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']) if x == element][0] + 1)

def getMonthDays(element, year):
    monrh_days = [31, 28, 31, 30, 31, 30, 31, 30, 30, 31, 30, 31]
    result = monrh_days[element - 1]
    if year % 4 == 0 & element == 2:
        result = 29
    return result

if __name__ == '__main__':
    generate_all_foodshare_reports()