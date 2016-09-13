#!/usr/bin/python
# -*- coding: UTF-8-*-
from __future__ import (absolute_import, division, print_function, unicode_literals)

import os
import sys
import numpy as np
import pandas as pd

from datetime import datetime, timedelta, date, time

# Ugly Hack
sys.path.append( os.path.dirname(os.path.dirname(
    os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ) ) ) )

from core.drive import generate_structure
from scripts import *

pd.options.mode.chained_assignment = None

class FoodShareReport(object):
    """docstring for FoodShareReport

    Example:

    foodshare = FoodShareReport()
    foodshare.generate_all_reports()

    The only public methods are :

        .generate_all_reports()
        .generate_report(ngo, programmes)

    """
    def __init__(self, **kwargs):
        super(FoodShareReport, self).__init__()

        self.ngo = None
        self.skip = []
        self.month = datetime.now().month
        self.year = datetime.now().year

        self.ROOT_FOLDER = 'data/Canonical/'
        self.REPORT_FOLDER = 'data/Reports/Funder/'

        self.FRESH_FOOD_CATEGORIES = ["Vegetable", "Leafy Veg", "Ground Veg",
                                 "Soy Products", "Fruit", "Bread", "Meat",
                                 "Seafood", "Cooked Food", "Fresh Other"]
        self.PACKAGED_FOOD_CATEGORIES = ["Staple", "Frozen", "Condiments",
                                    "Drinks", "Milk Powder", "Packaged Other"]

        self.MELT_INDEX = ['datetime', 'donor', 'organisation_id', 'programme']
        self.FINANCE_INDEX = ['month', 'income', 'expenditure']
        self.REPORT_INDEX = ['datetime', 'donor', 'organisation_id', 'programme', 'variable',
                        'value', 'canonical', 'donor_category', 'isFresh', 'year', 'month', 'day']

        self.STAGES = ['collection','processing','distribution']
        self.META_FILES_PROGRAMME = ['beneficiary','finance']
        self.META_FILES_NGO = ['donors']
        self.META_FILES = ['units','i18n','map']

        self.dest_path = None

        # Override the defaults
        if kwargs is not None:
            for k, v in kwargs.iteritems():
                setattr(self, k, v)

        self.REPORT_STARTDATE, self.REPORT_ENDDATE, self.REPORT_DURATION_MONTHS = self.get_report_dates()


    def get_report_dates(self):
        self.CURRENT_YEAR = self.year
        self.REPORT_PERIOD_STARTDATES = [datetime.strptime(d, "%d/%m").replace(
                                    year=self.CURRENT_YEAR).date() for d in ['1/4','1/9','1/12']]
        self.REPORT_PERIOD_ENDDATES   = [datetime.strptime(d, "%d/%m").replace(
                                    year=self.CURRENT_YEAR).date() for d in ['31/8','30/11','31/3']]
        self.REPORT_DURATION_MONTHS   = [5,3,4]

        # Silly Hack
        self. REPORT_PERIOD_STARTDATES[-1] = self.REPORT_PERIOD_STARTDATES[-1].replace(year=self.CURRENT_YEAR-1)

        # Most Recent Date
        min_index = lambda values: min(xrange(len(values)), key=values.__getitem__)
        period_index = min_index([datetime.now().date() - d if (datetime.now().date() - d).seconds > 0 else 99999 for d in self.REPORT_PERIOD_ENDDATES ])

        return map(lambda x : x[period_index], [self.REPORT_PERIOD_STARTDATES,
                                                self.REPORT_PERIOD_ENDDATES, self.REPORT_DURATION_MONTHS])


    def generate_all_reports(self):

        for ngo, programmes in generate_structure()['collection'].iteritems():

            # Selective processing of NGOS
            if ngo in ['FoodLink'] + self.skip:
                continue

            if ngo == self.ngo:

                for programme, sheets in programmes.iteritems():

                    # Selective processing of NGOS PROGRAMMES
                    # if ngo+programme in ['PSCSSP']:
                    #     continue

                    self.generate_report(ngo, programme)


    def generate_report(self, ngo, programme):

        print_status('generating', "{:8} {} - {}".format(self.ngo, str(self.REPORT_STARTDATE), str(self.REPORT_ENDDATE)), programme)

        self.ngo = ngo
        self.programme = programme

        self.dest_path = self.REPORT_FOLDER + ngo + " Funder Reports" + '/' + str(
            self.REPORT_ENDDATE) + '/'

        fns = self.available_csvs(ngo)
        try:
            df_map = self.map_csv_to_dataframe(ngo, fns, programme)
        except ValueError:
            print_status('failed',
                         "{:8} {} - {}".format(self.ngo, str(self.REPORT_STARTDATE), str(self.REPORT_ENDDATE)),
                         programme)
            return

        df_map.update(self.meta_csv_to_dataframe(ngo))

        # Collection
        df_map['collection'] = self.clean_df(df_map, 'collection')
        df_map['collection'] = self.melt_df(df_map, 'collection', self.MELT_INDEX)

        df_map['collection'] = self.df_merge(df_map, 'collection', 'map', 'variable', 'category')
        df_map['collection'] = self.df_merge(df_map, 'collection', 'donors', 'donor', 'id')
        try:
            df_map['collection'] = self.datetime_features(df_map, 'collection')
        except ValueError:
            print_status('failed',
                         "{:8} {} - {}".format(self.ngo, str(self.REPORT_STARTDATE), str(self.REPORT_ENDDATE)),
                         programme)
            return
        df_map['collection'] = self.clean_collection(df_map)

        df_map['collection'] = df_map['collection'][self.REPORT_INDEX].drop_duplicates()

        # Processing
        df_map['processing'] = self.clean_df(df_map, 'processing')
        try:
            df_map['processing'] = self.datetime_features(df_map, 'processing')
        except ValueError:
            print_status('failed',
                         "{:8} {} - {}".format(self.ngo, str(self.REPORT_STARTDATE), str(self.REPORT_ENDDATE)),
                         programme)
            return

        # Distribution
        df_map['distribution'] = self.clean_df(df_map, 'distribution')
        df_map['distribution'] = self.datetime_features(df_map, 'distribution')

        # Fincances
        df_map['finance'] = self.clean_finance(df_map)

        # Report
        df_map['report'] = self.report_template()
        df_map['report'] = self.generate_report_rows(df_map)

        self.report_to_excel(df_map['report'], ngo, programme)

# Data Processing

    def map_csv_to_dataframe(self, ngo, fns, programme):

        fns_in_programme = filter(lambda fn: programme in fn, fns)

        df_map = {}

        for stage in self.STAGES + self.META_FILES_PROGRAMME + self.META_FILES_NGO:
            fns = filter(lambda fn: stage in fn, fns_in_programme)
            path = self.ROOT_FOLDER + '/' + ngo + '/'
            df_map[stage] = pd.concat([pd.read_csv(path + fn, encoding='utf_8') for fn in sorted(fns)])

        # Clean Donors

        df_map['donors'] = self.clean_donors(df_map)

        return df_map


    def meta_csv_to_dataframe(self, ngo):
        metas = {}
        for meta in self.META_FILES:
            df = pd.read_csv(self.ROOT_FOLDER + meta + '.csv',encoding='utf_8')

            df = df[df.organisation_id == ngo]

            if meta == 'map':
                df = df[['category', 'canonical']]

            df = df.drop_duplicates()

            metas[meta] = df

        return metas


    def clean_df(self, df_map, key):
        df = df_map[key]
        df = self.df_slice_report_period(df, 'datetime')
        df = df.fillna(0)

        return df

    def clean_donors(self, df_map):
        df = df_map['donors']
        df = df[['id', 'foodshare_category']]
        df = df.rename(columns={'foodshare_category': 'donor_category'})
        df['donor_category'] = df['donor_category'].astype(basestring)

        # TODO : Check this line: WARNING | MART
        df = df[df['donor_category'].notnull()]

        return df

    def clean_collection(self, df_map):
        df = df_map['collection']
        df['isFresh'] = df.canonical.isin(self.FRESH_FOOD_CATEGORIES)
        return df

    def clean_finance(self, df_map):
        df = df_map['finance']

        try:
            df.columns = self.FINANCE_INDEX
        except ValueError as e:
            if 'Length mismatch' in str(e):
                msg = """Inspect the cover sheet for entries outside the
                    permitted columns. If you don't find anything, check
                    the weekly sheets for stray data entries. You will
                    need to re-export the source to fix this."""
                self.open_latest_sheet()
                print_data_error("INVALID DATA STRUCTURE", str(e),msg,exit=True)



        df['month_num'] = (df.index + 1)

        # ToDo Refactor | Mart

        if self.REPORT_STARTDATE.month == 4:
            finance_start = 8
        elif self.REPORT_STARTDATE.month == 9:
            finance_start = 4
        elif self.REPORT_STARTDATE.month == 12:
            finance_start = 13

        finance_end = finance_start - self.REPORT_DURATION_MONTHS
        df = df.iloc[-finance_start:-finance_end]

        try:
            df.expenditure = df.expenditure.astype('float')
        except ValueError as e:
            if 'could not convert string to float' in str(e):
                msg = """Search the source sheet for the invalid value
                    and correct it. You will need to re-export the
                    source to fix this."""
                self.open_latest_sheet()
                print_data_error("EXPECTED NUMBER, FOUND STRING", str(e), msg, exit=True)


        return df

    def melt_df(self, df_map, key, index_cols):
        df = df_map[key]
        rest_cols = [col for col in list(df.columns.values) if col not in index_cols]

        df = pd.melt(df, id_vars=index_cols, value_vars=rest_cols)
        df = df[df.value != 0]
        df = df[df['value'].notnull()]

        return df

    def df_slice_report_period(self, df, dt_key):
        df[dt_key] = pd.to_datetime(df[dt_key])

        start = df[dt_key].searchsorted(datetime.combine(self.REPORT_STARTDATE, time()))[0]
        end = df[dt_key].searchsorted(datetime.combine(self.REPORT_ENDDATE, time()))[0]

        return df.iloc[start:end]

    def df_merge(self, df_map, key_left, key_right, merge_key_left, merge_key_right):
        df = pd.merge(df_map[key_left], df_map[key_right], how='left', left_on=[merge_key_left], right_on=[merge_key_right])
        df = df.drop(merge_key_right, 1)

        return df

    def datetime_features(self, df_map, key):
        df = df_map[key]

        df['year'] = df.apply(lambda e: e.datetime.year, axis=1)
        df['month'] = df.apply(lambda e: e.datetime.month, axis=1)
        df['day'] = df.apply(lambda e: e.datetime.day, axis=1)
        return df

    def report_template(self):
        columns = ['element'] + map(str, range(1,13))
        df = pd.DataFrame(columns=columns)
        return df

    def generate_report_rows(self, df_map):

        # ToDo : Refactor | MART

        df = df_map['report']
        df_c = df_map['collection']
        df_p = df_map['processing']
        df_d = df_map['distribution']
        df_f = df_map['finance']

        df.loc[len(df)+1] = self.genRow('Total volume of food collected (kg)', df_c.groupby('month').value.agg(['sum'])['sum'])
        df.loc[len(df)+1] = self.genRow('Total volume of fresh food (kg)', df_c[df_c['isFresh'] == True].groupby('month').value.agg(['sum'])['sum'])
        df.loc[len(df)+1] = self.genRow('Total volume of packaged food (kg)', df_c[df_c['isFresh'] == False].groupby('month').value.agg(['sum'])['sum'])

        df.loc[len(df)+1] = self.genRow('Number of food rescue days/ month', df_c.groupby('month').day.nunique())
        df.loc[len(df)+1] = self.genRow('Total Donors Count', df_c.groupby('month').donor.agg(['count'])['count'])
        df.loc[len(df)+1] = self.genRow('Unique Donors Count', df_c.groupby('month').donor.nunique())

        df.loc[len(df)+1] = self.genRow('Total beneficiaries', df_d.groupby('month').kg.agg(['sum'])['sum'])

        df.loc[len(df)+1] = self.genRow('Compost volume (kg)', df_p.groupby('month').compost_amount.agg(['sum'])['sum'])
        df.loc[len(df)+1] = self.genRow('Disposal volume (kg)', df_p.groupby('month').disposal_amount.agg(['sum'])['sum'])
        df.loc[len(df)+1] = self.genRow('Storage volume (kg)', df_p.groupby('month').storage_amount.agg(['sum'])['sum'])

        df.loc[len(df)+1] = self.genRow('Donation/ Income ($)', df_f.groupby('month_num').income.agg(['sum'])['sum'])
        df.loc[len(df)+1] = self.genRow('Total expenditure ($)', df_f.groupby('month_num').expenditure.agg(['sum'])['sum'])

        df = df.fillna(0)

        df.loc[len(df)+1] = ['Total distribution volume (kg)'] + [a - e - f + g for a, e, f, g in zip(self.getList(df, 'Total volume of food collected (kg)'), self.getList(df, 'Compost volume (kg)'), self.getList(df, 'Disposal volume (kg)'), self.getList(df, 'Storage volume (kg)'))]
        df.loc[len(df)+1] = ['Percentage of food distributed for consumption (%)'] + [d / a for d, a in zip(self.getList(df, 'Total distribution volume (kg)'), self.getList(df, 'Total volume of food collected (kg)'))]
        df.loc[len(df)+1] = ['Compost Percentage (%)'] + [e / a for e, a in zip(self.getList(df, 'Compost volume (kg)'), self.getList(df, 'Total volume of food collected (kg)'))]
        df.loc[len(df)+1] = ['Disposal Percentage (%)'] + [f / a for f, a in zip(self.getList(df, 'Disposal volume (kg)'), self.getList(df, 'Total volume of food collected (kg)'))]
        df.loc[len(df)+1] = ['Storage Percentage (%)'] + [g / a for g, a in zip(self.getList(df, 'Storage volume (kg)'), self.getList(df, 'Total volume of food collected (kg)'))]
        df.loc[len(df)+1] = ['Average amount of food rescued/ day (kg)'] + [a / h for a , h in zip(self.getList(df, 'Total volume of food collected (kg)'), self.getList(df, 'Number of food rescue days/ month'))]
        df.loc[len(df)+1] = ['Average beneficiaries/day '] + [a / h for a , h in zip(self.getList(df, 'Total beneficiaries'), self.getList(df, 'Number of food rescue days/ month'))]
        df.loc[len(df)+1] = ['Average volume of food distributed/ per person  / day (kg)'] + [i / d / h for i , d, h in zip(self.getList(df, 'Total distribution volume (kg)'), self.getList(df, 'Number of food rescue days/ month'), self.getList(df, 'Total beneficiaries'))]
        df.loc[len(df)+1] = ['Average cost/ beneficiary ($)'] + [k / i for k , i in zip(self.getList(df, 'Total expenditure ($)'), self.getList(df, 'Total beneficiaries'))]

        df.loc[len(df)+1] = ['Average cost/kg of rescued food ($)'] + [k / i for k , i in zip(self.getList(df,'Total expenditure ($)'), self.getList(df, 'Total volume of food collected (kg)'))]
        df.loc[len(df)+1] = ['Average cost/ kg of distributed food ($)'] + [k / i for k , i in zip(self.getList(df, 'Total expenditure ($)'), self.getList(df, 'Total distribution volume (kg)'))]

        df = df.iloc[[0, 1, 2, 12, 13, 7, 14, 8, 15, 9, 16, 3, 17, 18, 19, 10, 11, 20, 21, 22, 4, 5]].copy()

        df.index = np.arange(1, len(df) + 1)

        for donor in pd.unique(df_c['donor_category']):
            if not isinstance(donor, basestring):
                donor = "MISSING"
            df.loc[len(df)+1] = self.genRow(donor.title() + ' (kg)', df_c[df_c['donor_category'] == donor].groupby('month').value.agg(['sum'])['sum'])

        df['Total'] = df.ix[:, 1:13].sum(axis=1)
        df['Average'] = df.ix[:, 1:13].mean(axis=1)

        return(df.fillna(0))

    # Utilities

    def available_csvs(self, ngo):
        try:
            return os.listdir(self.ROOT_FOLDER + ngo)
        except OSError:
            print_status('failed',
                         "{:8} {} - {}".format(self.ngo, str(self.REPORT_STARTDATE), str(self.REPORT_ENDDATE)),
                         self.programme)
            return []


    def find(self, the_series, the_value):
        return (''.join(map(str,[the_series for the_series, x in enumerate(the_series) if x == the_value])))

    def fixAllmth(self, the_series):
        the_list = []
        check_list = the_series.index.tolist()
        for mth in range(1, 13):
            if(self.find(check_list, mth).isdigit()):
                the_list = the_list + [the_series[mth]]
            else:
                the_list = the_list + [0]
        return the_list

    def getList(self, the_df, target_element):
        return the_df[the_df.element == target_element].ix[: , 1:].values[0]

    def genRow(self, the_name, the_series):
        the_series = self.fixAllmth(the_series)
        return [the_name] + the_series

    def getMonthNum(self, element):
        return ([i for i, x in enumerate(['Jan', 'Feb', 'Mar', 'Apr', 'May',
                                'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']) if x == element][0] + 1)

    def getMonthDays(self, element, year):
        month_days = [31, 28, 31, 30, 31, 30, 31, 30, 30, 31, 30, 31]
        result = month_days[element - 1]
        if year % 4 == 0 & element == 2:
            result = 29
        return result


    def report_to_excel(self, report, ngo, programme):
        name = "{}.{}.{}.{}".format('FoodShare', self.ngo, self.programme, self.REPORT_ENDDATE)
        sheet_name = name[name.index('.')+1:]
        dest_xlsx = self.dest_path + ".".join([name, 'report', 'xlsx'])

        if not os.path.exists(self.dest_path):
            os.makedirs(self.dest_path)

        report.to_excel(dest_xlsx, index_label='label', merge_cells=False, sheet_name = sheet_name)

        print_sub_header("DONE | {}".format(sheet_name), 'green',True)

    def open_latest_sheet(self):
        import json, webbrowser
        with open('structure.json') as fp:
            progress = json.load(fp)
            url_base = "https://docs.google.com/spreadsheets/d/"
            max_idx = 0
            max_name = ""
            for idx, sheet in enumerate(progress['processing'][self.ngo][self.programme]):
                if max(max_name, sheet['name']) == sheet['name']:
                    max_name = sheet['name']
                    max_idx = idx
            webbrowser.open_new(url_base + progress['processing'][self.ngo][self.programme][max_idx]['id'])


if __name__ == '__main__':
    report = FoodShareReport()
    report.generate_all_reports()
