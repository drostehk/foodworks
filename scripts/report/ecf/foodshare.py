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
REPORT_FOLDER = 'data/Report/'

FRESH_FOOD_CATEGORIES = ["Vegetable", "Leafy Veg", "Ground Veg", 
                         "Soy Products", "Fruit", "Bread", "Meat",
                         "Seafood", "Cooked Food", "Fresh Other"]
PACKAGED_FOOD_CATEGORIES = ["Staple", "Frozen", "Condiments", 
                            "Drinks", "Milk Powder", "Packaged Other"]

MELT_INDEX = ['datetime', 'donor', 'organisation_id', 'programme']
FINANCE_INDEX = ['month', 'income', 'expenditure']
REPORT_INDEX = ['datetime', 'donor', 'organisation_id', 'programme', 'variable',
                'value', 'canonical', 'donor_category', 'isFresh', 'year', 'month', 'day']

STAGES = ['collection','processing','distribution']
META_FILES_PROGRAMME = ['beneficiary','finance']
META_FILES_NGO = ['donors']
META_FILES = ['units','i18n','map']

def get_report_dates():
    CURRENT_YEAR = datetime.now().year
    REPORT_PERIOD_STARTDATES = [datetime.strptime(d, "%d/%m").replace(
                                year=CURRENT_YEAR).date()for d in ['1/4','1/9','1/12']]
    REPORT_PERIOD_ENDDATES   = [datetime.strptime(d, "%d/%m").replace(
                                year=CURRENT_YEAR).date() for d in ['31/8','30/11','31/3']]
    REPORT_DURATION_MONTHS   = [5,3,4]

    # Silly Hack
    REPORT_PERIOD_STARTDATES[-1] = REPORT_PERIOD_STARTDATES[-1].replace(year=CURRENT_YEAR-1)

    min_index = lambda values: min(xrange(len(values)), key=values.__getitem__)
    period_index  = min_index([d - datetime.now().date() for d in REPORT_PERIOD_ENDDATES])

    return map(lambda x : x[period_index], [REPORT_PERIOD_STARTDATES,
                                            REPORT_PERIOD_ENDDATES, REPORT_DURATION_MONTHS])

REPORT_STARTDATE, REPORT_ENDDATE, REPORT_DURATION_MONTHS = get_report_dates()

print('GENERATING REPORT FOR PERIOD\n ', REPORT_STARTDATE, '-', REPORT_ENDDATE)

def generate_all_foodshare_reports(year=datetime.now().year):

    for ngo, programmes in generate_structure()['collection'].iteritems():

        # Selective processing of NGOS
        if ngo in ['FoodLink', 'NLPRA','ActionHealth']:
            continue

        for programme, sheets in programmes.iteritems():

            # Selective processing of NGOS PROGRAMMES
            if ngo+programme in ['PSCSSP']:
                continue

            generate_foodshare_report(ngo, programme)


def generate_foodshare_report(ngo, programme):
    fns = available_csvs(ngo)
    df_map = map_csv_to_dataframe(ngo, fns, programme)

    df_map.update(meta_csv_to_dataframe(ngo))

    # Collection
    df_map['collection'] = clean_df(df_map, 'collection')
    df_map['collection'] = melt_df(df_map, 'collection', MELT_INDEX)

    df_map['collection'] = df_merge(df_map, 'collection', 'map', 'variable', 'category')
    df_map['collection'] = df_merge(df_map, 'collection', 'donors', 'donor', 'id')

    df_map['collection'] = datetime_features(df_map, 'collection')
    df_map['collection'] = clean_collection(df_map)
    
    df_map['collection'] = df_map['collection'][REPORT_INDEX].drop_duplicates()

    # Processing
    df_map['processing'] = clean_df(df_map, 'processing')
    df_map['processing'] = datetime_features(df_map, 'processing')
    
    # Distribution
    df_map['distribution'] = clean_df(df_map, 'distribution')
    df_map['distribution'] = datetime_features(df_map, 'distribution')

    # Fincances
    df_map['finance'] = clean_finance(df_map)

    # Report
    df_map['report'] = report_template()
    df_map['report'] = generate_report_rows(df_map)

    report_to_excel(df_map['report'], ngo, programme)

# Data Processing 

def map_csv_to_dataframe(ngo, fns, programme):

    fns_in_programme = filter(lambda fn: programme in fn, fns)

    df_map = {}
        
    for stage in STAGES + META_FILES_PROGRAMME + META_FILES_NGO:
        fns = filter(lambda fn: stage in fn, fns_in_programme)
        path = ROOT_FOLDER + '/' + ngo + '/'
        df_map[stage] = pd.concat([pd.read_csv(path + fn, encoding='utf_8') for fn in sorted(fns)])

    # Clean Donors

    df_map['donors'] = clean_donors(df_map)

    return df_map


def meta_csv_to_dataframe(ngo):
    metas = {}
    for meta in META_FILES:
        df = pd.read_csv(ROOT_FOLDER + meta + '.csv',encoding='utf_8')

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

    # TODO : Check this line: WARNING | MART
    df = df[df['donor_category'].notnull()]

    return df

def clean_collection(df_map):
    df = df_map['collection']
    df['isFresh'] = df.canonical.isin(FRESH_FOOD_CATEGORIES)
    return df

def clean_finance(df_map):
    df = df_map['finance']
    df.columns = FINANCE_INDEX
    df['month_num'] = (df.index + 1)
    
    # ToDo Refactor | Mart

    if REPORT_STARTDATE.month == 4:
        finance_start = 8
    elif REPORT_STARTDATE.month == 9:
        finance_start = 4
    elif REPORT_STARTDATE.month == 12:
        finance_start = 13

    # import pdb
    # pdb.set_trace()

    finance_end = finance_start - REPORT_DURATION_MONTHS
    df = df.iloc[-finance_start:-finance_end]

    df.expenditure = df.expenditure.astype('float')

    REPORT_STARTDATE.month
    
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
    df = df.drop(merge_key_right, 1)

    return df

def datetime_features(df_map, key):
    df = df_map[key]

    df['year'] = df.apply(lambda e: e.datetime.year, axis=1)
    df['month'] = df.apply(lambda e: e.datetime.month, axis=1)
    df['day'] = df.apply(lambda e: e.datetime.day, axis=1)
    return df

def report_template():
    columns = ['element'] + map(str, range(1,13))
    df = pd.DataFrame(columns=columns)
    return df

def generate_report_rows(df_map):
    
    # ToDo : Refactor | MART
    
    df = df_map['report']
    df_c = df_map['collection']
    df_p = df_map['processing']
    df_d = df_map['distribution']
    df_f = df_map['finance']
    
    df.loc[len(df)+1] = genRow('Total volume of food collected (kg)', df_c.groupby('month').value.agg(['sum'])['sum'])
    df.loc[len(df)+1] = genRow('Total volume of fresh food (kg)', df_c[df_c['isFresh'] == True].groupby('month').value.agg(['sum'])['sum'])
    df.loc[len(df)+1] = genRow('Total volume of packaged food (kg)', df_c[df_c['isFresh'] == False].groupby('month').value.agg(['sum'])['sum'])

    df.loc[len(df)+1] = genRow('Number of food rescue days/ month', df_c.groupby('month').day.nunique())
    df.loc[len(df)+1] = genRow('Total Donors Count', df_c.groupby('month').donor.agg(['count'])['count'])
    df.loc[len(df)+1] = genRow('Unique Donors Count', df_c.groupby('month').donor.nunique())


    df.loc[len(df)+1] = genRow('Total beneficiaries', df_d.groupby('month').Distribution_Count.agg(['sum'])['sum'])

    df.loc[len(df)+1] = genRow('Compost volume (kg)', df_p.groupby('month').compost.agg(['sum'])['sum'])
    df.loc[len(df)+1] = genRow('Disposal volume (kg)', df_p.groupby('month').disposal.agg(['sum'])['sum'])
    df.loc[len(df)+1] = genRow('Storage volume (kg)', df_p.groupby('month').storage.agg(['sum'])['sum'])

    # import pdb
    # pdb.set_trace()

    df.loc[len(df)+1] = genRow('Donation/ Income ($)', df_f.groupby('month_num').income.agg(['sum'])['sum'])
    df.loc[len(df)+1] = genRow('Total expenditure ($)', df_f.groupby('month_num').expenditure.agg(['sum'])['sum'])

    df = df.fillna(0)

    df.loc[len(df)+1] = ['Total distribution volume (kg)'] + [a - e - f + g for a, e, f, g in zip(getList(df, 'Total volume of food collected (kg)'), getList(df, 'Compost volume (kg)'), getList(df, 'Disposal volume (kg)'), getList(df, 'Storage volume (kg)'))]
    df.loc[len(df)+1] = ['Percentage of food distributed for consumption (%)'] + [d / a for d, a in zip(getList(df, 'Total distribution volume (kg)'), getList(df, 'Total volume of food collected (kg)'))]
    df.loc[len(df)+1] = ['Compost Percentage (%)'] + [e / a for e, a in zip(getList(df, 'Compost volume (kg)'), getList(df, 'Total volume of food collected (kg)'))]
    df.loc[len(df)+1] = ['Disposal Percentrage (%)'] + [f / a for f, a in zip(getList(df, 'Disposal volume (kg)'), getList(df, 'Total volume of food collected (kg)'))]
    df.loc[len(df)+1] = ['Storage Percentage (%)'] + [g / a for g, a in zip(getList(df, 'Storage volume (kg)'), getList(df, 'Total volume of food collected (kg)'))]
    df.loc[len(df)+1] = ['Average amount of food rescued/ day (kg)'] + [a / h for a , h in zip(getList(df, 'Total volume of food collected (kg)'), getList(df, 'Number of food rescue days/ month'))]
    df.loc[len(df)+1] = ['Average beneficiaries/day '] + [a / h for a , h in zip(getList(df, 'Total beneficiaries'), getList(df, 'Number of food rescue days/ month'))]
    df.loc[len(df)+1] = ['Average volume of food distributed/ per person  / day (kg)'] + [i / d / h for i , d, h in zip(getList(df, 'Total distribution volume (kg)'), getList(df, 'Number of food rescue days/ month'), getList(df, 'Total beneficiaries'))]
    df.loc[len(df)+1] = ['Average cost/ beneficiary ($)'] + [k / i for k , i in zip(getList(df, 'Total expenditure ($)'), getList(df, 'Total beneficiaries'))]

    df.loc[len(df)+1] = ['Average cost/kg of rescued food ($)'] + [k / i for k , i in zip(getList(df, 'Total expenditure ($)'), getList(df, 'Total volume of food collected (kg)'))]
    df.loc[len(df)+1] = ['Average cost/ kg of distributed food ($)'] + [k / i for k , i in zip(getList(df, 'Total expenditure ($)'), getList(df, 'Total distribution volume (kg)'))]

    df = df.iloc[[0, 1, 2, 12, 13, 7, 14, 8, 15, 9, 16, 3, 17, 18, 19, 10, 11, 20, 21, 22, 4, 5]].copy()

    df.index = np.arange(1, len(df) + 1)

    # import pdb
    # pdb.set_trace()

    for donor in pd.unique(df_c['donor_category']):
        if not isinstance(donor, basestring):
            donor = "MISSING"
        df.loc[len(df)+1] = genRow(donor.title() + ' (kg)', df_c[df_c['donor_category'] == donor].groupby('month').value.agg(['sum'])['sum'])

    df['Total'] = df.ix[:, 1:13].sum(axis=1)
    df['Average'] = df.ix[:, 1:13].mean(axis=1)

    return(df.fillna(0))

# Utilities 

def available_csvs(ngo):
    return os.listdir(ROOT_FOLDER + ngo)

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


def report_to_excel(report, ngo, programme):
    name = ".".join([ngo, programme, str(REPORT_ENDDATE)])
    dest_xlsx = REPORT_FOLDER + ".".join([name, 'report', 'xlsx'])

    if not os.path.exists(REPORT_FOLDER):
        os.makedirs(REPORT_FOLDER)

    print('Report generating to: ' + dest_xlsx)
    report.to_excel(dest_xlsx, index_label='label', merge_cells=False, sheet_name = name)
    print('Done!')

if __name__ == '__main__':
    generate_all_foodshare_reports()