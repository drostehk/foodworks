#!/usr/bin/python
# -*- coding: UTF-8-*-
from __future__ import (absolute_import, division, print_function, unicode_literals)

import os
import sys
import pdb
import numpy as np
import pandas as pd
import operator as op

# TODO FEATURES 

# Make sure that the collection and distribution amounts add up - #23

from datetime import datetime, timedelta, date, time

# Ugly Hack
sys.path.append( os.path.dirname(os.path.dirname(
    os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ) ) ) )

# from core.drive import generate_structure

class ECFReport(object):
    """docstring for ECFReport

    Example:

    ecf = ECFReport()
    ecf.generate_all_reports()

    The only public methods are :

        .generate_all_reports()
        .generate_single_reports(ngo,stage,progammes)

    """
    def __init__(self, **kwargs):
        super(ECFReport, self).__init__()

        self.ROOT_FOLDER = 'data/Canonical/'
        self.REPORT_FOLDER = 'data/Report/'

        self.FRESH_FOOD_CATEGORIES = ["Vegetable", "Leafy Veg", "Ground Veg", 
                                 "Soy Products", "Fruit", "Bread", "Meat",
                                 "Seafood", "Cooked Food", "Fresh Other"]
        self.PACKAGED_FOOD_CATEGORIES = ["Staple", "Frozen", "Condiments", 
                                    "Drinks", "Milk Powder", "Packaged Other"]

        self.COLLECTION_CATEGORIES_TABS = ['Cooked &- Fruit','Food Products']
        self.COLLECTION_CATEGORIES = ['Cooked Food','Food Product']

        self.STAGES = ['collection',]
        self.STAGE_TITLE = {
            'collection': 'Recovery',
            'distribution': 'Distribution'
        }

        self.MELT_INDEX = ['datetime', 'donor', 'organisation_id', 'programme']
        self.FINANCE_INDEX = ['month', 'income', 'expenditure']
        self.REPORT_INDEX = ['datetime', 'donor', 'organisation_id', 'programme', 'variable',
                        'value', 'canonical', 'donor_category', 'isFresh', 'year', 'month', 'day']

        self.META_FILES_PROGRAMME = []
        self.META_FILES_NGO = ['donors']
        self.META_FILES = ['map']

        self.PERIOD = (datetime.now() - timedelta(days=28))
        self.MONTH_NUM = self.PERIOD.month
        self.MONTH_NAME = self.PERIOD.strftime('%B')
        self.YEAR_NUM = self.PERIOD.year

        # WHITE & BLACKLISTING

        # SKIP_NGO = ['NLPRA']
        self.SKIP_NGO = []
        self.ONLY_NGO = []

        # SKIP_STAGES = ['collection']
        self.SKIP_STAGES = []
        self.ONLY_STAGES = []

        self.SKIP_PROGRAMMES = [u'General',u'Amenities']
        self.ONLY_PROGRAMMES = ['ECF Van 01','ECF Van 02','ECF Van 03']

        # Override the defaults
        if kwargs is not None:
            for k, v in kwargs.iteritems():
                setattr(self, k, v)
        
        self.ngo = ''
        self.stage = ''
        self.programmes = []
    

    def generate_all_reports(self, year=datetime.now().year, iteration=1):

        print('GENERATING REPORT FOR PERIOD\n ', self.YEAR_NUM, '-', self.MONTH_NUM)

        print('\n### LOADING DRIVE STRUCTURE### {}'.format(iteration))
        
        for stage, ngos in generate_structure().iteritems():

            print('\n### ITERATION ### {}'.format(iteration))
            # Selective processing of stages
            if stage in self.SKIP_STAGES:
                continue

            # Exclusive processing of stages
            if self.ONLY_STAGES and not stage in self.ONLY_STAGES:
                continue

            self.stage = stage
            
            for ngo, programmes in ngos.iteritems():

                # Selective processing of NGOS
                if ngo in self.SKIP_NGO:
                    continue

                # Exclusive processing of NGOS
                if self.ONLY_NGO and not ngo in self.ONLY_NGO:
                    print("### SKIP {} ###".format(ngo.upper()))
                    continue

                self.ngo = ngo

                if not programmes:
                    continue


                programmes = [p for p in programmes.keys()
                    if p in self.ONLY_PROGRAMMES]

                self.programmes = programmes
                
                print(programmes)
                
                self.generate_report()

    
    def generate_single_report(self, ngo, stage, programmes):
        self.ngo = ngo
        self.stage = stage
        self.programmes = programmes
        self.generate_report()        
    
    
    # PRIVATE METHODS 

    def generate_report(self):

        fns = self.available_csvs()
        
        df_map = {}
        df_map[self.stage] = self.map_source_to_dataframe(fns)

        df_map['donors'] = self.map_donors_to_dataframe(fns)
        df_map['meta'] = self.map_donors_to_dataframe(fns)

        import pdb; pdb.set_trace()

        # Collection
        stage = self.stage

        if stage is 'collection':
            df_map[stage] = self.clean_df(df_map, stage)
            df_map[stage] = self.melt_df(df_map, stage, MELT_INDEX)

            df_map[stage] = self.df_merge(df_map, 'collection', 'map', 'variable', 'category')
            df_map[stage] = self.df_merge(df_map, 'collection', 'donors', 'donor', 'id')

            df_map[stage] = self.datetime_features(df_map, 'collection')
            df_map[stage] = self.clean_collection(df_map)
            
            df_map[stage] = df_map['collection'][REPORT_INDEX].drop_duplicates()

        # Processing
        # NOT USED

        # Distribution
        
        if stage is 'distribution':
            df_map[stage] = self.clean_df(df_map)
            df_map[stage] = self.datetime_features(df_map)

        # Fincances
        # NOT USED

        # Report
        df_map['report'] = self.report_template()
        df_map['report'] = self.generate_report_rows(df_map)

        self.report_to_excel(df_map['report'])

    # Data Processing 

    def map_source_to_dataframe(self, fns):

        is_selected = lambda fn : any(map(lambda p: p in fn, self.programmes))

        fns_in_stage = sorted(filter(lambda fn: self.stage in fn, fns))
        fns = sorted([fn for fn in fns_in_stage if is_selected(fn)])

        df = pd.concat([pd.read_csv(self.base_path() + fn,
            encoding='utf_8') for fn in fns_in_stage])

        return self.clean_source(df)


    def map_donors_to_dataframe(self, fns):

        is_selected = lambda fn : any(map(lambda p: p in fn, self.programmes))

        donor_fns = [fn for fn in fns if 'donors' in fn]

        dfs = []
        for programme in self.programmes:
            path = self.base_path() + next((fn for fn in donor_fns if programme in fn), None)
            df = pd.read_csv(path, encoding='utf_8')
            df['programme'] = programme
            df.drop_duplicates(inplace=True)
            dfs.append(df)

        return self.clean_donors(pd.concat(dfs))

    def map_beneficiaries_to_dataframe(self, fns):
        pass

    def map_meta_to_dataframe(self, fns):
        metas = {}
        for meta in META_FILES:
            df = pd.read_csv(ROOT_FOLDER + meta + '.csv',encoding='utf_8')

            df = df[df.organisation_id == ngo]

            if meta == 'map':
                df = df[['category', 'canonical']]

            df = df.drop_duplicates()

            metas[meta] = df

        return metas


    def clean_source(self, df):
        period = "{}/{}".format(self.MONTH_NUM, self.YEAR_NUM)
        df.datetime = pd.to_datetime(df.datetime)
        df.sort_values(by=['datetime', 'programme', 'donor'], inplace=True)
        df.set_index('datetime').truncate(before=period, after=period).reset_index()
        df.fillna(0, inplace=True)
        return df

    def clean_donors(self, df):

        df = df[['id', 'foodshare_category']]
        df = df.rename(columns={'foodshare_category': 'donor_category'})
        df['donor_category'] = df['donor_category'].astype(basestring)

        # TODO : Check this line: WARNING | MART
        df = df[df['donor_category'].notnull()]

        return df

    def clean_collection(self, df):
        df['isFresh'] = df.canonical.isin(FRESH_FOOD_CATEGORIES)
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

        start = df[dt_key].searchsorted(datetime.combine(REPORT_STARTDATE, time()))[0]
        end = df[dt_key].searchsorted(datetime.combine(REPORT_ENDDATE, time()))[0]
        
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

    def report_template(self, ):
        columns = ['element'] + map(str, range(1,13))
        df = pd.DataFrame(columns=columns)
        return df

    def generate_report_rows(self, df_map):
        
        # ToDo : Refactor | MART
        
        # df = df_map['report']
        # df_c = df_map['collection']
        # df_d = df_map['distribution']
        
        # df.loc[len(df)+1] = genRow('Total volume of food collected (kg)', df_c.groupby('month').value.agg(['sum'])['sum'])
        # df.loc[len(df)+1] = genRow('Total volume of fresh food (kg)', df_c[df_c['isFresh'] == True].groupby('month').value.agg(['sum'])['sum'])
        # df.loc[len(df)+1] = genRow('Total volume of packaged food (kg)', df_c[df_c['isFresh'] == False].groupby('month').value.agg(['sum'])['sum'])

        # df.loc[len(df)+1] = genRow('Number of food rescue days/ month', df_c.groupby('month').day.nunique())
        # df.loc[len(df)+1] = genRow('Total Donors Count', df_c.groupby('month').donor.agg(['count'])['count'])
        # df.loc[len(df)+1] = genRow('Unique Donors Count', df_c.groupby('month').donor.nunique())


        # df.loc[len(df)+1] = genRow('Total beneficiaries', df_d.groupby('month').Distribution_Count.agg(['sum'])['sum'])

        # df.loc[len(df)+1] = genRow('Compost volume (kg)', df_p.groupby('month').compost.agg(['sum'])['sum'])
        # df.loc[len(df)+1] = genRow('Disposal volume (kg)', df_p.groupby('month').disposal.agg(['sum'])['sum'])
        # df.loc[len(df)+1] = genRow('Storage volume (kg)', df_p.groupby('month').storage.agg(['sum'])['sum'])

        # # import pdb
        # # pdb.set_trace()

        # df.loc[len(df)+1] = genRow('Donation/ Income ($)', df_f.groupby('month_num').income.agg(['sum'])['sum'])
        # df.loc[len(df)+1] = genRow('Total expenditure ($)', df_f.groupby('month_num').expenditure.agg(['sum'])['sum'])

        # df = df.fillna(0)

        # df.loc[len(df)+1] = ['Total distribution volume (kg)'] + [a - e - f + g for a, e, f, g in zip(getList(df, 'Total volume of food collected (kg)'), getList(df, 'Compost volume (kg)'), getList(df, 'Disposal volume (kg)'), getList(df, 'Storage volume (kg)'))]
        # df.loc[len(df)+1] = ['Percentage of food distributed for consumption (%)'] + [d / a for d, a in zip(getList(df, 'Total distribution volume (kg)'), getList(df, 'Total volume of food collected (kg)'))]
        # df.loc[len(df)+1] = ['Compost Percentage (%)'] + [e / a for e, a in zip(getList(df, 'Compost volume (kg)'), getList(df, 'Total volume of food collected (kg)'))]
        # df.loc[len(df)+1] = ['Disposal Percentrage (%)'] + [f / a for f, a in zip(getList(df, 'Disposal volume (kg)'), getList(df, 'Total volume of food collected (kg)'))]
        # df.loc[len(df)+1] = ['Storage Percentage (%)'] + [g / a for g, a in zip(getList(df, 'Storage volume (kg)'), getList(df, 'Total volume of food collected (kg)'))]
        # df.loc[len(df)+1] = ['Average amount of food rescued/ day (kg)'] + [a / h for a , h in zip(getList(df, 'Total volume of food collected (kg)'), getList(df, 'Number of food rescue days/ month'))]
        # df.loc[len(df)+1] = ['Average beneficiaries/day '] + [a / h for a , h in zip(getList(df, 'Total beneficiaries'), getList(df, 'Number of food rescue days/ month'))]
        # df.loc[len(df)+1] = ['Average volume of food distributed/ per person  / day (kg)'] + [i / d / h for i , d, h in zip(getList(df, 'Total distribution volume (kg)'), getList(df, 'Number of food rescue days/ month'), getList(df, 'Total beneficiaries'))]
        # df.loc[len(df)+1] = ['Average cost/ beneficiary ($)'] + [k / i for k , i in zip(getList(df, 'Total expenditure ($)'), getList(df, 'Total beneficiaries'))]

        # df.loc[len(df)+1] = ['Average cost/kg of rescued food ($)'] + [k / i for k , i in zip(getList(df, 'Total expenditure ($)'), getList(df, 'Total volume of food collected (kg)'))]
        # df.loc[len(df)+1] = ['Average cost/ kg of distributed food ($)'] + [k / i for k , i in zip(getList(df, 'Total expenditure ($)'), getList(df, 'Total distribution volume (kg)'))]

        # df = df.iloc[[0, 1, 2, 12, 13, 7, 14, 8, 15, 9, 16, 3, 17, 18, 19, 10, 11, 20, 21, 22, 4, 5]].copy()

        # df.index = np.arange(1, len(df) + 1)

        # # import pdb
        # # pdb.set_trace()

        # for donor in pd.unique(df_c['donor_category']):
        #     if not isinstance(donor, basestring):
        #         donor = "MISSING"
        #     df.loc[len(df)+1] = genRow(donor.title() + ' (kg)', df_c[df_c['donor_category'] == donor].groupby('month').value.agg(['sum'])['sum'])

        # df['Total'] = df.ix[:, 1:13].sum(axis=1)
        # df['Average'] = df.ix[:, 1:13].mean(axis=1)

        # return(df.fillna(0))
        pass

    # Utilities 

    def base_path(self):
        return self.ROOT_FOLDER + self.ngo + '/'

    def available_csvs(self):
        return os.listdir(self.base_path())

    def find(self, the_series, the_value):
        return (''.join(map(str,[the_series for the_series, x in enumerate(the_series) if x == the_value])))

    def fixAllmth(self, the_series):
        the_list = []
        check_list = the_series.index.tolist()
        for mth in range(1, 13):
            if(find(check_list, mth).isdigit()):
                the_list = the_list + [the_series[mth]]
            else:
                the_list = the_list + [0]
        return the_list

    def getList(self, the_df, target_element):
        return the_df[the_df.element == target_element].ix[: , 1:].values[0]

    def genRow(self, the_name, the_series):
        the_series = fixAllmth(the_series)
        return [the_name] + the_series

    def getMonthNum(self, element):
        return ([i for i, x in enumerate(['Jan', 'Feb', 'Mar', 'Apr', 'May',
                                'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']) if x == element][0] + 1)

    def getMonthDays(self, element, year):
        monrh_days = [31, 28, 31, 30, 31, 30, 31, 30, 30, 31, 30, 31]
        result = monrh_days[element - 1]
        if year % 4 == 0 & element == 2:
            result = 29
        return result

    def report_to_excel(self, report):
        # Reports/<FORMAT>/<ORG>.<YEAR>.<?MONTH?>.report.xlsx
        xls_header = '{} Record of Food {} Activities ({}) - {}'.format(datetime.now().year, self.stage, self.STAGE_TITLE[self.stage], self.MONTH_NAME)

        name = ".".join([self.ngo, str(self.YEAR_NUM), str(self.MONTH_NUM), self.stage])
        dest_xlsx = self.REPORT_FOLDER + ".".join([name, 'ecf', 'report', 'xlsx'])

        if not os.path.exists(self.REPORT_FOLDER):
            os.makedirs(self.REPORT_FOLDER)

        print('Report generating to: ' + dest_xlsx)
        report.to_excel(dest_xlsx, index_label='label', merge_cells=True, sheet_name=self.STAGE_TITLE[self.stage])
        print('Done!')

if __name__ == '__main__':
    opts = {
        "ONLY_NGO" : ['FoodLink'],
        'ONLY_STAGES' : ['collection']
    }
    report = ECFReport(**opts)
    report.generate_all_reports()