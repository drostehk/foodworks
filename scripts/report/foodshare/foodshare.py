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

MELT_INDEX = ['datetime', 'donor', 'organisation_id', 'programme']
    
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

    df_map.update(meta_csv_to_dataframe(ngo))

    # Collection
    df_map['collection'] = clean_df(df_map, 'collection')
    df_map['collection'] = df_merge(df_map, 'collection', 'map', 'variable', 'category')
    df_map['collection'] = df_merge(df_map, 'collection', 'donors', 'donor', 'id')

    df_map['collection'] = melt_df(df_map, 'collection', MELT_INDEX)

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

    # Clean Donors

    df_map['donors'] = clean_donors()

    return df_map



def meta_csv_to_dataframe(ngo):
    metas = {}
    for meta in META_FILES:
        df = pd.read_csv(ROOT_FOLDER + meta + '.csv')

        df = df[df.organisation_id == ngo]

        if meta == 'map':
            df = df[['category', 'canonical']]

        df = df.drop_duplicates()

        metas[meta] = df

    return metas


def clean_df(df_map, key):
    df = df_map[key]
    df = df_slice_report_period(df,'datetime')
    df = df.fillna(0)
   
    return df

def clean_donors(df_map):
    df = df_map['donors']
    df = df[['id', 'foodshare_category']]
    df = df.rename(columns={'foodshare_category': 'donor_category'})
    df['donor_category'] = df['donor_category'].astype(basestring)

    return df

def melt_df(df_map, key, index_cols):
    df = df_map[key] 
    rest_cols = [col for col in list(df.columns.values) if col not in index_cols]
    df = pd.melt(df, id_vars=index_cols, value_vars=rest_cols)
    df = df[df.value != 0]
    df = df[df['value'].notnull()]
    
    return df

def df_slice_report_period(df, dt_key):
    df[dt_key] = pd.to_datetime(df[dt_key])

    start = df[dt_key].searchsorted(datetime.combine(REPORT_STARTDATE, time()))[0]
    end = df[dt_key].searchsorted(datetime.combine(REPORT_ENDDATE, time()))[0]
    
    return df.iloc[start:end]

def df_merge(df_map, key_left, key_right, merge_key_left, merge_key_right):
    df = pd.merge(df_map[key_left], df_map[key_right], how='left', left_on=[merge_key_left], right_on=[merge_key_right])
    df = df_merge.drop(merge_key_right, 1)

    return df


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