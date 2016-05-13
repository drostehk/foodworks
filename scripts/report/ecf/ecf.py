#!/usr/bin/python
# -*- coding: UTF-8-*-
from __future__ import (absolute_import, division, print_function, unicode_literals)

import os
import sys
import pdb
import numpy as np
import pandas as pd
import operator as op
import xlsxwriter


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

        
        self.COLLECTION_CATEGORIES_TABS = ['Cooked & Fruit','Food Products']
        self.COLLECTION_CATEGORIES = ['Cooked Food','Food Product']

        self.STAGES = ['collection',]
        self.STAGE_TITLE = {
            'collection': 'Recovery',
            'distribution': 'Distribution'
        }

        self.SORT_KEY = ['datetime','programme','donor']
        self.MELT_INDEX = ['datetime', 'donor', 'programme']
        self.FINANCE_INDEX = ['month', 'income', 'expenditure']
        self.REPORT_INDEX = ['datetime', 'donor', 'organisation_id', 'programme', 'variable',
                        'value', 'canonical', 'donor_category', 'year', 'month', 'day']

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
        self.ONLY_PROGRAMMES = ['ECF Van 01','ECF Van 02','ECF Van 03','General']

        # Override the defaults
        if kwargs is not None:
            for k, v in kwargs.iteritems():
                setattr(self, k, v)
        
        self.ngo = ''
        self.stage = ''
        self.programmes = []
    
        print('GENERATING REPORT FOR PERIOD\n ', self.YEAR_NUM, '-', self.MONTH_NUM)

    def generate_all_reports(self, year=datetime.now().year, iteration=1):

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

        # df_map['meta'] = self.map_meta_to_dataframe(fns)

        # Collection
        stage = self.stage


        if stage == 'collection':
            df_map['donors'] = self.map_donors_to_dataframe(fns)
            
            df_map[stage] = self.melt_df(df_map[self.stage])

            df_merge = df_map[stage].merge(df_map['donors'],how='left',
                left_on=['donor','programme'],right_on=['id','programme'])

            cols = ['datetime','programme','name_en','location','variable','value']
            df = df_merge[cols]
            df.columns = ['datetime','programme','donor','address','category','kg']
            
            df.datetime = df.datetime.dt.strftime('%d-%b-%y')

            df = df.sort_values(by=self.SORT_KEY)
            df = df.set_index(self.SORT_KEY)
        
            self.report_to_excel(df)

        # TODO Split up into Cooked Food // Packaged

        # Processing
        # NOT USED

        # Distribution
        
        if stage is 'distribution':
            df_map['beneficiaries'] = self.map_beneficiaries_to_dataframe(fns)
            df_map[stage] = self.datetime_features(df_map)

        # Fincances
        # NOT USED


    # Data Processing 

    def map_source_to_dataframe(self, fns):

        is_selected = lambda fn : any(map(lambda p: p in fn, self.programmes))

        fns_in_stage = sorted(filter(lambda fn: self.stage in fn, fns))
        fns = sorted([fn for fn in fns_in_stage if is_selected(fn)])

        df = pd.concat([pd.read_csv(self.base_path() + fn,
            encoding='utf_8') for fn in fns])

        return self.clean_source(df)


    def map_donors_to_dataframe(self, fns):

        is_selected = lambda fn : any(map(lambda p: p in fn, self.programmes))

        donor_fns = [fn for fn in fns if 'donors' in fn and is_selected(fn)]

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
        df.drop('organisation_id', axis=1, inplace=True)
        df.datetime = pd.to_datetime(df.datetime)
        df.sort_values(by=['datetime', 'programme', 'donor'], inplace=True)
        # TODO FIX THE TRUNCATE
        df.set_index('datetime').truncate(before=period, after=period)
        df.fillna(0, inplace=True)
        return df

    def clean_donors(self, df):
        cols = ['id', 'name_en', 'location', 'programme']
        df = df[cols]
        return df

    # XLXWRITER

    def report_to_excel(self, df):
        # Reports/<FORMAT>/<ORG>.<YEAR>.<?MONTH?>.report.xlsx

        # for n, df in enumerate(dfs):
            # df.to_excel(writer, self.COLLECTION_CATEGORIES_TABS[n])

        name = ".".join([self.ngo, str(self.YEAR_NUM), str(self.MONTH_NUM), self.stage])
        sheet_name = self.COLLECTION_CATEGORIES_TABS[0]
        
        dest_xlsx = self.REPORT_FOLDER + ".".join([name, 'ecf', 'report', 'xlsx'])
        xls_header = '{} Record of Food {} Activities ({}) - {}'.format(
            datetime.now().year, self.stage, self.STAGE_TITLE[self.stage], self.MONTH_NAME)

        print('Report generating to: ' + dest_xlsx)

        self.ensure_dest_exists()

        writer = pd.ExcelWriter(dest_xlsx, engine='xlsxwriter')
        pd.DataFrame(['']).to_excel(writer, sheet_name, startrow=0,
                        index=False, header=False)
        df.to_excel(writer, sheet_name, startrow=1, merge_cells=True, )
        
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        worksheet.merge_range('A1:G1', xls_header)

        self.write_totals_column(df, workbook, worksheet)
        self.set_col_widths(workbook, worksheet)

        writer.save()
        
        print('\n Done!')


    def write_totals_column(self, df, workbook, worksheet):

        format = self.set_workbook_format(workbook)
        
        for i, r in self.get_totals_column(df).iterrows():
            try:
                worksheet.merge_range("G{row_start}:G{row_end}".format(**r), r.kg, format)
            except UserWarning as e:
                print(e)
                worksheet.write_number("G{row_start}".format(**r), r.kg, format)


    def get_totals_column(self, df):
        offset = 2
        totals = df.groupby(level='datetime').sum()
        rowspan = df.ix[:,-1:].groupby(level='datetime').count()
        totals['span'] = rowspan
        totals['row_end'] = (totals['span'].cumsum() + offset).astype('int')
        # totals['row_end'] = totals['row_end'] - range(1,len(totals)+1)
        totals['row_start'] = (totals['row_end'].shift().fillna(offset).astype('int') + 1).astype('int')
        return totals


    def set_workbook_format(self, workbook):
        format = workbook.add_format()
        format.set_align('center')
        format.set_align('bottom')
        return format


    def set_col_widths(self, workbook, worksheet):
        col_widths = [15,12,45,70,12,5,10]

        format = workbook.add_format()
        format.set_align('center')
        format.set_align('vcenter')
        format.set_font('Courier New')

        for col, width in enumerate(col_widths):
            worksheet.set_column(col, col, width, cell_format=format) 

    # Utilities 

    def base_path(self):
        return self.ROOT_FOLDER + self.ngo + '/'

    def available_csvs(self):
        return os.listdir(self.base_path())

    def melt_df(self, df):
        rest_cols = [col for col in list(df.columns.values) if col not in self.MELT_INDEX]

        df = pd.melt(df, id_vars=self.MELT_INDEX, value_vars=rest_cols)
        df = df[df.value != 0]
        df = df[df['value'].notnull()]

        return df

    def ensure_dest_exists(self):
        if not os.path.exists(self.REPORT_FOLDER):
            os.makedirs(self.REPORT_FOLDER)



if __name__ == '__main__':
    opts = {
        "ONLY_NGO" : ['FoodLink'],
        'ONLY_STAGES' : ['collection','distribution']
    }
    report = ECFReport(**opts)
    report.generate_all_reports()

    # programmes = [u'ECF Van 01', u'ECF Van 03', u'ECF Van 02']
    # report.generate_single_report('FoodLink', 'collection', programmes);