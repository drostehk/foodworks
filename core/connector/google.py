#!/usr/bin/python
# -*- coding: UTF-8-*-
from __future__ import (absolute_import, division, print_function, unicode_literals)
import datetime
import numpy as np
import pandas as pd
from gspread import Client
from gspread.ns import _ns
from gspread.models import Spreadsheet
from gspread.exceptions import SpreadsheetNotFound
from foodworks.credentials import getGoogleCredentials


import seaborn


class GoogleSourceClient(Client):
    """Connected for Google Sheets"""

    _ss_prefix = 'FoodWorks Data'

    def __init__(self, auth):
        """Specify which data source to connect to"""
        super(GoogleSourceClient, self).__init__(auth)

    def open_source(self, org, stage="Collection", year=datetime.datetime.now().year, programme=""):
        """Opens a spreadsheet, returning a :class:`~foodworks.connectors.google.GoogleSourceSheet` instance.

        If there's more than one spreadsheet with same title the first one
        will be opened.

        :raises gspread.SpreadsheetNotFound: if no spreadsheet with
                                             specified `title` is found.

        # >>> gc = GoogleSourceClient.connect()
        # >>> ss = gc.open_source('TSWN','Collection',2015)
        """

        # Programme is optional and only used when NGOs split out their operations into multiple programmes
        if programme is not "":
            programme = '' + programme

        ss_name = "{} - {} {} {}{}".format(self._ss_prefix, stage, org, year, programme)
        feed = self.get_spreadsheets_feed()

        for elem in feed.findall(_ns('entry')):
            elem_title = elem.find(_ns('title')).text
            if elem_title.strip() == ss_name:
                return GoogleSourceSheet(self, elem, org, stage, year)
        else:
            raise SpreadsheetNotFound

    @classmethod
    def connect(cls, credentials=getGoogleCredentials()):
        """Login to Google API using OAuth2 credentials.

        This is a shortcut function which instantiates :class:`Client`
        and performs login right away.


        :returns: :class:`Client` instance.

        # >>> gc = GoogleSourceClient.connect()
        # >>>
        """
        client = cls(credentials)
        client.login()
        return client


class GoogleSourceSheet(Spreadsheet):

    def __init__(self, client, feed_entry, organisation, stage="Collection", year=datetime.datetime.now().year):
        super(GoogleSourceSheet, self).__init__(client, feed_entry)

        self.org = organisation
        self.stage = stage
        self.year = year

        self.chinese_weekdays = [u'星期一', u'星期二', u'星期三', u'星期四', u'星期五', u'星期六', u'星期日']
        self.chinese_type = u'其他'
        self.chinese_volume = u'其他 (公斤)'
        
        if(stage == "Collection"):
            self.std_cols = ['organisation_id', 'programme', 'datetime', 'donor']
        elif(stage == "Distribution"):
            #self.std_cols = ['datetime', 'Beneglciary_id', 'Distribution_Count', 'Distribution_Amount']
            self.std_cols = ['datetime', 'Beneglciary_id', 'Distribution_Count']
        elif(stage == "Processing"):
            self.std_cols = ['datetime', 'na_1', 'na_2','compost', 'disposal', 'storage']
        else:
            raise NotImplementedError

        self.df = pd.DataFrame(columns=self.std_cols)

        self.set_schema()
        self.parse_meta_sheet()

    def parse_cover_sheet(self):
        if self.stage in ['Collection', 'Distribution', 'Processing']:
            ws = self.get_worksheet(0)
            values = ws.get_all_values()
            cover = pd.DataFrame(values)
            cover.columns = cover.iloc[0]
            cover = cover.ix[1:]
            return cover
        '''    
                                    elif self.stage == 'Processing':
                                        ws = self.get_worksheet(0)
                                        values = ws.get_all_values()
                                        cover = pd.DataFrame(values)
                                        cover.columns = cover.iloc[0]
                                        cover = cover.ix[1:]
                                        return cover
        '''    

    def collect_week_sheets(self):
        return [ws for ws in self.worksheets() if _is_week_number(ws.title)]

    def weekday_to_date(self, monday_date_as_str , chinese_week_day):
        monday_date = datetime.datetime.strptime(monday_date_as_str, '%Y-%m-%d')
        days_offset = self.chinese_weekdays.index(chinese_week_day)
        record_date = monday_date + datetime.timedelta(days=days_offset)
        return record_date

    def parse_meta_sheet(self):
        ws = self.worksheet('meta')
        metadata = dict(zip(ws.row_values(1), ws.row_values(2)))
        self.schema_version = metadata['schema_version']
        if self.stage == 'Collection':
            self.programme = metadata['programme']

    def set_schema(self):
        ws = self.worksheet('1')
        if self.stage == 'Collection':
            col_headers = ws.get_all_values()[1][2:]
            schema_items = col_headers[:col_headers.index(self.chinese_type)]
            schema_items = schema_items + col_headers[col_headers.index(self.chinese_volume)+1:-2]
            self.col_headers = col_headers
            self.schema = schema_items
        elif self.stage == 'Distribution':
            schema_items = self.std_cols
            self.col_headers = self.std_cols
            self.schema = self.std_cols
        elif self.stage == 'Processing':
            schema_items = self.std_cols
            self.col_headers = self.std_cols
            self.schema = self.std_cols
        else:
            raise NotImplementedError

    def parse_collection(self):
        wss = self.collect_week_sheets()
        self.df = self.df.join(pd.DataFrame(columns=self.schema))
        for ws in wss:
            self.parse_collection_weeksheet(ws)

        self.df.datetime = pd.to_datetime(self.df.datetime)

        terms = self.df.columns[4:].tolist()

        self.create_translations_keys(terms)
        self.create_mappings_keys(terms)
        self.create_units_keys(terms)

        return self.df

    def parse_collection_weeksheet(self, ws):
        print('Parsing Week', ws.title)
        header_offset = 2
        values = ws.get_all_values()
        collection = pd.DataFrame(values)

        collection.columns = collection.iloc[1].tolist()
        donors = collection.iloc[header_offset:, 1]
        timestamps = collection.iloc[header_offset:, 0]
        col_categories = collection.iloc[0, 2:]
        raw_df = collection.ix[header_offset:, self.schema]
        raw_df.columns = self.schema
        loc = len(self.df)
        for ridx, row in raw_df.iterrows():
            donor = donors[ridx]
            timestamp = self.weekday_to_date(collection.iloc[0,1], timestamps[ridx])
            # TODO: Ignore if there are no values in the standard row.
            self.df.loc[loc, self.std_cols + self.schema] = self.standard_row(donor, timestamp, row.tolist())
            loc += 1
        collection = collection.replace('', np.nan, regex=True)
        bool_mask = collection[collection.columns[-1]].notnull().apply(any, axis=1)
        bool_mask[0:2] = False
        for ridx, row in collection.loc[bool_mask].iterrows():
            timestamp = self.weekday_to_date(collection.iloc[0,1], row[0])
            donor = row[1]
            for idx, food_type in enumerate(row[self.chinese_type]):
                if food_type is not np.nan:
                    food_volume = row[self.chinese_volume][idx]
                    self.df.ix[(self.df.donor == donor) & (self.df.datetime == timestamp), food_type.strip()] = float(food_volume)

    def standard_row(self, donor, timestamp, std_values):
        return [self.org, self.programme, timestamp, donor] + std_values

    def create_translations_keys(self, terms):
        self.terms_to_sheet('Translations', terms)

    def create_mappings_keys(self, terms):
        self.terms_to_sheet('Mappings', terms)

    def create_units_keys(self, terms):
        self.terms_to_sheet('Units', terms)

    def terms_to_sheet(self, sheet_name, terms):
        ssx = self.client.open("{} - {}".format(self.client._ss_prefix, sheet_name))
        ws = ssx.get_worksheet(0)

        orgs = ws.col_values(1)
        schema_versions = ws.col_values(2)
        programmes = ws.col_values(3)
        zh_terms = np.array(ws.col_values(4))

        find = lambda match, group: [i for i, x in enumerate(group) if x == match]

        org_refs = find(self.org, orgs)
        programme_refs = find(self.programme, programmes)
        schema_refs = find(self.schema_version, schema_versions)

        current_refs = set(org_refs).intersection(set(programme_refs)).intersection(schema_refs)

        if len(current_refs) == 0:
            for term in terms:
                print(sheet_name, '>>>', term)
                ws.append_row([self.org, self.schema_version, self.programme, term])
        else:
            existing_terms = zh_terms[list(current_refs)].tolist()
            new_terms = set(terms).difference(existing_terms)
            if len(new_terms) > 0:
                for term in new_terms:
                    print(sheet_name, '>>> ', term)
                    ws.append_row([self.org, self.schema_version, self.programme, term])

    def collect_terms_sheets(self):
        sheet_names = {
                'i18n' : 'Translations',
                'map' : 'Mappings',
                'units' : 'Units'
            }

        for code, name in sheet_names.iteritems():
            ssx = self.client.open("{} - {}".format(self.client._ss_prefix, name))

            ws = ssx.get_worksheet(0)

            yield ws.get_all_values(), code


    def parse_processing(self):
        wss = self.collect_week_sheets()
        for ws in wss:
            self.parse_processing_weeksheet(ws)
        return self.df

    def parse_distribution(self):
        wss = self.collect_week_sheets()
        for ws in wss:
            self.parse_dist_weeksheet(ws)
        return self.df

    def parse_dist_weeksheet(self, ws):
        header_offset = 2
        values = ws.get_all_values()
        collection = pd.DataFrame(values)
        print('Parsing Week', ws.title, '' if(len(collection.index) > header_offset) else  '(No Record)')
        #print(collection.iloc[header_offset:, 0])

        if(len(collection.index) > header_offset):
            timestamps = collection.iloc[header_offset:, 0]
            raw_df = collection.ix[header_offset:, :].copy()
            raw_df.columns = self.schema
            loc = len(self.df)
            tempList = []
            for ridx in timestamps.index.tolist():
                timestamp = self.weekday_to_date(collection.iloc[0,1], timestamps[ridx])
                tempList = tempList + [timestamp]
            raw_df['datetime'] = tempList
            self.df = self.df.append(raw_df,ignore_index = True)    

    def parse_processing_weeksheet(self, ws):
        header_offset = 2
        values = ws.get_all_values()
        collection = pd.DataFrame(values)
        print('Parsing Week', ws.title, '' if(len(collection.index) > header_offset) else  '(No Record)')
        #print(collection.iloc[header_offset:, 0])

        if(len(collection.index) > header_offset):
            timestamps = collection.iloc[header_offset:, 0]
            raw_df = collection.ix[header_offset:, :].copy()
            #print(raw_df.columns.values)
            raw_df.columns = self.schema
            loc = len(self.df)

            #print(raw_df)
            #print(timestamps)
            #print(collection.iloc[0,1])
            tempList = []
            for ridx in timestamps.index.tolist():
                timestamp = self.weekday_to_date(collection.iloc[0,1], timestamps[ridx])
                tempList = tempList + [timestamp]
            raw_df['datetime'] = tempList
            self.df = self.df.append(raw_df,ignore_index = True)    


def _is_week_number(title):
    if not title.isdigit():
        return False
    elif int(title) < 1:
        return False
    elif int(title) > 53:
        return False
    else:
        return True
