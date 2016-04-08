#!/usr/bin/python
# -*- coding: UTF-8-*-
from __future__ import (absolute_import, division, print_function, unicode_literals)

import os
import sys
import datetime
import numpy as np
import pandas as pd
import operator as op

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

def generate_all_foodshare_reports(year=datetime.datetime.now().year):

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

    import pprint
    pprint.pprint(df_map)


def map_csv_to_dataframe(ngo, fns, programme):

    fns_in_programme = filter(lambda fn: programme in fn, fns)

    df_map = {}
        
    for stage in STAGES + META_FILES_PROGRAMME + META_FILES_NGO:
        fns = filter(lambda fn: stage in fn, fns_in_programme)
        path = ROOT_FOLDER + '/' + ngo + '/'
        df_map[stage] = pd.concat([pd.read_csv(path + fn) for fn in fns]).shape

    return df_map

def available_csvs(ngo):
    return os.listdir(ROOT_FOLDER + ngo)






# Utilities 

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