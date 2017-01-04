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
import calendar
from scripts import *

import pdb

import warnings
warnings.filterwarnings("error")

# TODO FEATURES 

# Make sure that the collection and distribution amounts add up - #23

from datetime import datetime, timedelta, date, time

# Ugly Hack
sys.path.append( os.path.dirname(os.path.dirname(
    os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ) ) ) )

from core.drive import generate_structure

class ECFReport(object):
    """docstring for ECFReport

    Example:

    ecf = ECFReport()
    ecf.generate_all_reports()

    The only public methods are :

        .generate_all_reports()
        .generate_single_reports(ngo,stage,programmes)

    """
    def __init__(self, **kwargs):
        super(ECFReport, self).__init__()

        self.ROOT_FOLDER = 'data/Canonical/'
        self.REPORT_FOLDER = 'data/Reports/Funder/'
        
        self.STAGES = ['collection','distribution']
        self.STAGE_TITLE = {
            'collection': 'Recovery',
            'distribution': 'Distribution'
        }

        self.SORT_KEY = []
        self.MELT_INDEX = ['datetime', 'donor', 'programme']
        self.FINANCE_INDEX = ['month', 'income', 'expenditure']
        self.REPORT_INDEX = ['datetime', 'donor', 'organisation_id', 'programme', 'variable',
                        'value', 'canonical', 'donor_category', 'year', 'month', 'day']

        self.META_FILES_PROGRAMME = []
        self.META_FILES_NGO = ['donors']
        self.META_FILES = ['map']

        # Dates

        self.PERIOD = (datetime.now() - timedelta(days=28))
        self.MONTH_NUM = self.PERIOD.month
        self.MONTH_NAME = self.PERIOD.strftime('%B')
        self.YEAR_NUM = self.PERIOD.year

        self.start_date = datetime(self.YEAR_NUM, self.MONTH_NUM, 1)
        self.end_date = datetime(self.YEAR_NUM, self.MONTH_NUM, calendar.monthrange(
            self.YEAR_NUM, self.MONTH_NUM)[1])

        # WHITE & BLACKLISTING

        # SKIP_NGO = ['NLPRA']
        self.SKIP_NGO = []
        self.ONLY_NGO = []

        # SKIP_STAGES = ['collection']
        self.SKIP_STAGES = []
        self.ONLY_STAGES = []

        self.SKIP_PROGRAMMES = [u'Amenities']
        self.ONLY_PROGRAMMES = None
        self.ONLY_PROGRAMMES = ['ECF Van 01','ECF Van 02','ECF Van 03','General']

        self.ngo = ''
        self.stage = ''
        self.programmes = []

        # Override the defaults
        if kwargs is not None:
            for k, v in kwargs.iteritems():
                setattr(self, k, v)

        self.meal_weight = 0.42
    
    def generate_all_reports(self, year=datetime.now().year, iteration=1):

        for stage, ngos in generate_structure().iteritems():

            # print('\n### ITERATION ### {}'.format(iteration))
            # Selective processing of stages
            if stage in self.SKIP_STAGES:
                continue

            # Exclusive processing of stages
            if self.ONLY_STAGES and not stage in self.ONLY_STAGES:
                continue

            self.stage = stage
            
            for ngo, programmes in ngos.iteritems():

               if ngo == self.ngo:

                    if not programmes:
                        continue

                    programmes = [p for p in programmes.keys()]

                    self.programmes = programmes

                    self.generate_report()

    
    def generate_single_report(self, ngo, stage, programmes):
        self.ngo = ngo
        self.stage = stage
        self.programmes = programmes
        self.generate_report()        
    
    
    # PRIVATE METHODS 

    def generate_report(self):

        pd.set_option("display.max_rows", 500)

        fns = self.available_csvs()
        
        df_map = {}
        df_map[self.stage] = self.map_source_to_dataframe(fns)

        # df_map['meta'] = self.map_meta_to_dataframe(fns)

        # Collection
        stage = self.stage

        if stage == 'collection':

            self.SORT_KEY = ['datetime','programme','donor']

            df_map['donors'] = self.map_orgs_to_dataframe(fns, 'donor')

            df_map[stage] = self.melt_df(df_map[self.stage])

            df_merge = df_map[stage].merge(df_map['donors'],how='left',
                left_on=['donor','programme'],right_on=['id','programme'])

            cols = ('datetime','programme','name_en','location','variable','value')
            df = df_merge.loc[:, cols]
            df.columns = ['datetime','programme','donor','address','category','kg']
            
            df.datetime = df.datetime.dt.strftime('%d-%b-%y')

            df = self.slice_reporting_month(df)

            df = df.sort_values(by=self.SORT_KEY)
            df = df.set_index(self.SORT_KEY)

            self.report_to_excel(df)

        # TODO Split up into Cooked Food // Packaged

        # Processing
        # NOT USED

        # Distribution
        if stage == 'distribution':

            self.SORT_KEY = ['datetime','programme','beneficiary']

            df_map['beneficiaries'] = self.map_orgs_to_dataframe(fns, 'beneficiary')

            df_merge = df_map[stage].merge(df_map['beneficiaries'],how='left',
                left_on=['beneficiary','programme'],right_on=['id','programme'])

            cols = ['datetime','programme','name_en','location','category','kg']
            df = df_merge.loc[:,cols]
            df.columns = ['datetime','programme','beneficiary','address','category','kg']
            df['meals'] = df['kg'] / self.meal_weight
            
            df.datetime = df.datetime.dt.strftime('%d-%b-%y')

            df = self.slice_reporting_month(df)

            df = df.sort_values(by=self.SORT_KEY)
            df = df.set_index(self.SORT_KEY)

            self.report_to_excel(df)

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


    def map_orgs_to_dataframe(self, fns, org_type):

        is_selected = lambda fn : any(map(lambda p: p in fn, self.programmes))

        fns = [fn for fn in fns if org_type in fn and is_selected(fn)]

        dfs = []
        for programme in self.programmes:
            path = self.base_path() + next((fn for fn in fns if programme in fn), None)
            df = pd.read_csv(path, encoding='utf_8')
            df['programme'] = programme
            df.drop_duplicates(inplace=True)
            dfs.append(df)

        return self.clean_orgs(pd.concat(dfs))


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
        org = {
            'collection' : 'donor',
            'distribution' : 'beneficiary'
        }
        df[org[self.stage]] = df[org[self.stage]].str.lower()
        df.sort_values(by=['datetime', 'programme', org[self.stage]], inplace=True)
        # TODO FIX THE TRUNCATE
        df.set_index('datetime').truncate(before=period, after=period)
        df.fillna(0, inplace=True)
        return df

    def clean_orgs(self, df):
        cols = ['id', 'name_en', 'location', 'programme']
        df = df[cols].copy()
        df.id = df.id.str.lower()
        return df

    # XLXWRITER

    def report_to_excel(self, df):
        opts = {}
        opts['name'] = ".".join([self.ngo, str(self.YEAR_NUM), str(self.MONTH_NUM), self.STAGE_TITLE[self.stage]])
        opts['sheet_name'] = self.STAGE_TITLE[self.stage]
        dir_path = "/".join([self.ngo + " Funder Reports", str(self.YEAR_NUM), str(self.MONTH_NUM)])
        opts['dest_xlsx'] = self.REPORT_FOLDER + dir_path + '/' + ".".join([opts['name'], 'ECF', 'Report', 'xlsx'])
        opts['xls_header'] = '{} Record of Food {} Activities ({}) - {}'.format(
            datetime.now().year, self.stage, self.STAGE_TITLE[self.stage], self.MONTH_NAME)
        
        self.ensure_dest_exists(self.REPORT_FOLDER + dir_path + '/')

        print_status('generating', "{:8} {} - {}".format(self.ngo, str(self.YEAR_NUM), self.MONTH_NAME), self.stage)

        if self.stage == 'collection':
            self.report_collection_to_excel(df, **opts)
            
        if self.stage == 'distribution':
            self.report_distribution_to_excel(df, **opts)

    def report_collection_to_excel(self, df, name, sheet_name, dest_xlsx, xls_header):
        
        writer = pd.ExcelWriter(dest_xlsx, engine='xlsxwriter')
        # Leave space for header
        pd.DataFrame(['']).to_excel(writer, sheet_name, startrow=0,
                        index=False, header=False)
        # Write Dataframe to sheet
        df.to_excel(writer, sheet_name, startrow=1, merge_cells=True)
        
        # Write header
        workbook = writer.book
        
        format = self.set_index_format(workbook)

        worksheet = writer.sheets[sheet_name]
        worksheet.merge_range('A1:G1', xls_header, format)

        headers = ['Date', 'Programme', 'Company / Organisation', 'Address', 'Category of Food ',
            'Quantity of Food Delivered (kg)','Daily Total (kg)']

        worksheet.write_row('A2', headers, format)

        # Write Totals Column
        self.write_sum_column(df, workbook, worksheet, 'kg', 'G')

        # Styling
        self.set_col_widths(workbook, worksheet)

        writer.save()

        print_sub_header("DONE | {}".format(sheet_name), 'green', True)


    def report_distribution_to_excel(self, df, name, sheet_name, dest_xlsx, xls_header):

        writer = pd.ExcelWriter(dest_xlsx, engine='xlsxwriter')
        # Leave space for header
        pd.DataFrame(['']).to_excel(writer, sheet_name, startrow=0,
                        index=False, header=False)
        # Write Dataframe to sheet
        df.to_excel(writer, sheet_name, startrow=1, merge_cells=True)
        
        # Write header
        workbook = writer.book
        
        format = self.set_index_format(workbook)

        worksheet = writer.sheets[sheet_name]
        worksheet.merge_range('A1:I1', xls_header, format)

        headers = ['Date', 'Programme', 'Company / Organisation', 'Address', 'Category of Food ',
            'Quantity of Food Delivered (kg)', 'Number of Meals',
            'Daily Total (kg)', 'Daily Number of Meals']

        worksheet.write_row('A2', headers, format)

        # Write Daily Totals Column
        self.write_sum_column(df, workbook, worksheet, 'kg', 'H')
        
        # Write Number of Daily Meals Columns
        self.write_sum_column(df, workbook, worksheet, 'meals', 'I')

        # Styling
        self.set_col_widths(workbook, worksheet)

        writer.save()

        print_sub_header("DONE | {}".format(sheet_name), 'green', True)


    def write_sum_column(self, df, workbook, worksheet, col, dest_col):

        # ToDo infer dest_col from df width

        format = self.set_workbook_format(workbook)
        
        for i, r in self.get_totals_column(df, col).iterrows():
            try:
                worksheet.merge_range("{}{}:{}{}".format(dest_col, r['row_start'], dest_col, r['row_end']), r[col], format)
            except UserWarning as e:
                worksheet.write_number(dest_col+str(int(r['row_end'])), float(r[col]), format)


    def get_totals_column(self, df, col):
        offset = 2
        totals = df.groupby(level='datetime').sum()
        totals['span'] = df.ix[:,col].groupby(level='datetime').count()
        totals['row_end'] = (totals['span'].cumsum() + offset).astype('int')
        totals['row_start'] = (totals['row_end'].shift().fillna(offset).astype('int') + 1).astype('int')
        return totals


    def set_workbook_format(self, workbook):
        format = workbook.add_format()
        format.set_align('center')
        format.set_align('bottom')
        format.set_num_format(0x01)
        return format

    def set_index_format(self, workbook):
        format = workbook.add_format()
        format.set_bold()
        format.set_text_wrap()
        format.set_align('center')
        format.set_align('vcenter')
        return format


    def set_col_widths(self, workbook, worksheet):
        col_widths = [15,12,50,75,18,16,16,16,16]

        format = workbook.add_format()
        format.set_align('center')
        format.set_align('vcenter')
        format.set_font('Courier New')
        format.set_num_format(0x01) 

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

    def ensure_dest_exists(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def slice_reporting_month(self, df):
        mask = (pd.to_datetime(df.datetime) >= self.start_date) & (pd.to_datetime(df.datetime) <= self.end_date)
        return df[mask]



if __name__ == '__main__':
    opts = {
        "ONLY_NGO" : ['FoodLink'],
        'ONLY_STAGES' : ['collection','distribution']
    }
    report = ECFReport(**opts)
    report.generate_all_reports()

    # programmes = [u'ECF Van 01', u'ECF Van 03', u'ECF Van 02']
    # report.generate_single_report('FoodLink', 'collection', programmes);
