#!/usr/bin/python
# -*- coding: UTF-8-*-
from __future__ import (absolute_import, division, print_function, unicode_literals)

import re
import sys, os
import datetime
import numpy as np
import pandas as pd

from gspread import Client
from gspread.ns import _ns
from gspread.models import Spreadsheet
from gspread.exceptions import SpreadsheetNotFound
from gspread.utils import *

from ..credentials import getGoogleCredentials

sys.path.append( os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ) )
from scripts import print_status

_url_key_re_v1 = re.compile(r'key=([^&#]+)')
_url_key_re_v2 = re.compile(r'spreadsheets/d/([^&#]+)/edit')

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
            programme = ' - ' + programme

        ss_name = "{} - {} {} {}{}".format(self._ss_prefix, stage, org, year, programme)
        feed = self.get_spreadsheets_feed()

        for elem in feed.findall(_ns('entry')):
            elem_title = elem.find(_ns('title')).text
            if elem_title.strip() == ss_name:
                return GoogleSourceSheet(self, elem, org, stage, year)
        else:
            raise SpreadsheetNotFound

    def open_by_key(self, key, stage, org, year):
        """Opens a spreadsheet specified by `key`, returning a :class:`~gspread.Spreadsheet` instance.
        :param key: A key of a spreadsheet as it appears in a URL in a browser.
        :raises gspread.SpreadsheetNotFound: if no spreadsheet with
                                             specified `key` is found.
        >>> c = gspread.Client(auth=('user@example.com', 'qwertypassword'))
        >>> c.login()
        >>> c.open_by_key('0BmgG6nO_6dprdS1MN3d3MkdPa142WFRrdnRRUWl1UFE')
        """

        feed = self.get_spreadsheets_feed()
        for elem in feed.findall(_ns('entry')):
            alter_link = finditem(lambda x: x.get('rel') == 'alternate',
                                  elem.findall(_ns('link')))
            m = _url_key_re_v1.search(alter_link.get('href'))
            if m and m.group(1) == key:
                return GoogleSourceSheet(self, elem, org, stage, year)

            m = _url_key_re_v2.search(alter_link.get('href'))
            if m and m.group(1) == key:
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

        # FoodShare Settings
        self.chinese_weekdays = [u'星期一', u'星期二', u'星期三', u'星期四', u'星期五', u'星期六', u'星期日']
        self.chinese_other_type = u'其他'
        self.chinese_other_volume = u'其他 (公斤)'
        self.chinese_other_header_cols = 2
        self.chinese_other_value_cols = -1

        # ECF Settings
        self.english_weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        self.english_other_type = 'Other'
        self.english_other_volume = u'Other (KG)'
        self.english_other_header_cols = 5
        self.english_other_value_cols = -4
        self.english_unit = u'Unit'
        self.english_quantity = u'Quantity'
        self.english_unit_weight = u'Unit Weight'

        # Default Settings 
        self.parsing_code = 'foodshare'
        self.language = 'zh_HK'
        self.weekdays = self.chinese_weekdays
        self.other_type = self.chinese_other_type
        self.other_volume = self.chinese_other_volume
        self.other_header_cols = self.chinese_other_header_cols
        self.other_value_cols = self.chinese_other_value_cols
        self.unit = None
        self.quantity = None
        self.unit_weight = None
        self.std_cols = []

        self.parse_meta_sheet()
        self.set_std_cols()
        self.set_schema()
        self.set_df()

        # Shared
        self.wk = None


    def parse_meta_sheet(self):
        ws = self.worksheet('meta')
        metadata = dict(zip(ws.row_values(1), ws.row_values(2)))
        self.schema_version = metadata['schema_version']

        # TODO Break this up into seperate parsing subclasses instead of
        # having the logic literred throughout this one class

        # Language Settings 
        if 'language' in metadata:
            self.language = metadata['language']
            if metadata['language'] == 'en':
                for val in ['weekdays', 'other_type', 'other_volume', 'unit', 'quantity', 'unit_weight']:
                    setattr(self, val, getattr(self,'english_'+val))
        if 'parsing_code' in metadata:
            self.parsing_code = metadata['parsing_code']
            if metadata['parsing_code'] == 'ecf':
                print('PARSING CODE : ECF')
                self.other_header_cols = self.english_other_header_cols
                self.other_value_cols = self.english_other_value_cols
        
        # Programme Settings 
        # TODO ECF also has Programmes in other stages.

        if self.stage in ['Collection','Distribution']:
            try:
                self.programme = metadata['programme']
            except KeyError:
                self.programme = 'General'


    def set_std_cols(self):
        if(self.stage == "Collection"):
            self.std_cols = ['organisation_id', 'programme', 'datetime', 'donor']
        elif(self.stage == "Distribution"):
            if(self.parsing_code == 'ecf'):
                self.std_cols = ['organisation_id','programme','datetime', 'beneficiary']
                self.export_cols = ['organisation_id','programme','datetime', 'beneficiary','category', 'kg']
            else:
                self.std_cols = ['organisation_id','programme','datetime', 'beneficiary_id',]
                self.export_cols = ['organisation_id','programme','datetime', 'beneficiary', 'kg']
        elif(self.stage == "Processing"):
            self.std_cols = ['datetime', 'na_1', 'na_2', 'compost_amount', 'disposal_amount', 'storage_amount']
        else:
            raise NotImplementedError
   

    def set_df(self):
        self.df = pd.DataFrame(columns=self.std_cols)


    def set_schema(self):
        ws = self.worksheet('1')
        if self.stage == 'Collection':
            col_headers = ws.get_all_values()[1][2:]
            idx = col_headers.index(self.other_type)
            schema_items = col_headers[:idx]

            offset = self.other_header_cols
            idx = col_headers.index(self.other_volume) + 1
            schema_items += col_headers[idx:-offset]

            self.col_headers = col_headers
            self.schema = schema_items
        
        elif self.stage == 'Distribution':
            self.index_cols = ['weekday','beneficiary']
            if (self.parsing_code == 'ecf'):
                self.col_headers = ['category', 'distribution_amount']
                self.schema = ['category', 'Volume (KG)']
            else:
                self.col_headers = ['distribution_amount']
                self.schema = ['分發人次']

        elif self.stage == 'Processing':
            self.col_headers = self.std_cols
            self.schema = self.std_cols

        else:
            raise NotImplementedError


    def parse_cover_sheet(self):
        if self.stage in ['Collection', 'Distribution', 'Processing']:
            ws = self.get_worksheet(0)
            values = ws.get_all_values()
            cover = pd.DataFrame(values)
            cover.columns = cover.iloc[0]
            cover = cover.ix[1:]
            return cover


    def parse_collection(self):

        wss = self.collect_week_sheets()
        self.df = self.df.join(pd.DataFrame(columns=self.schema))
        
        for ws in wss:
            self.parse_collection_weeksheet(ws)

        self.df.datetime = pd.to_datetime(self.df.datetime)

        terms = self.df.columns[4:].tolist()

        self.create_translations_keys(terms)
        self.create_mappings_keys(terms)


        # Nobody is using units 
        # self.create_units_keys(terms)
        print('\n')
        return self.df

    def parse_collection_weeksheet(self, ws):
        self.wk = ws.title
        header_offset = 2
        values = ws.get_all_values()
        try:
            if not any(values[2]):
                print_status("Skipped", 'Weekly Sheet |   0 rows', ws.title)
                return
        except IndexError:
            return
        
        collection = pd.DataFrame(self.get_data_rows(values))
        print_status("Parsing", 'Weekly Sheet | {:>3} rows'.format(len(collection)), ws.title)

        collection.columns = collection.iloc[1].tolist()
        donors = collection.iloc[header_offset:, 1]
        timestamps = collection.iloc[header_offset:, 0]
        
        raw_df = collection.ix[header_offset:, self.schema]
        raw_df.columns = self.schema
        loc = len(self.df)

        for ridx, row in raw_df.iterrows():
            donor = donors[ridx]
            timestamp = self.weekday_to_date(collection.iloc[0,1], timestamps[ridx])
            self.df.loc[loc, self.std_cols + self.schema] = self.standard_row(donor, timestamp, row.tolist())
            loc += 1

        collection = collection.replace('', np.nan, regex=True)
        bool_mask = collection[[collection.columns[self.other_value_cols]]].notnull().apply(any, axis=1)
        bool_mask[0:2] = False

        for ridx, row in collection.loc[bool_mask].iterrows():
            timestamp = self.weekday_to_date(collection.iloc[0,1], row[0])
            donor = row[1]
            for idx, food_type in enumerate(row[self.other_type]):
                if food_type is not np.nan:
                    food_volume = row[self.other_volume][idx]
                    donor_idx = (self.df.donor == donor)
                    date_idx = (self.df.datetime == timestamp)
                    food_idx = food_type.strip()
                    if food_idx in self.df and not any(self.df.ix[donor_idx & date_idx, food_idx].isnull()):
                        self.df.ix[donor_idx & date_idx, food_idx] += float(food_volume)
                    else:
                        self.df.ix[donor_idx & date_idx, food_idx] = float(food_volume)

        self.df.ix[:, self.schema] = self.df[self.schema].replace('',np.nan).astype(np.float)
        self.df = self.df.drop_duplicates().groupby(['organisation_id','programme','datetime','donor'], as_index=False, squeeze=False).sum()

    def parse_processing(self):
        wss = self.collect_week_sheets()
        # DEVELOPER
        for ws in wss:
            self.parse_processing_weeksheet(ws)
        print('\n')
        return self.df

    def parse_processing_weeksheet(self, ws):
        self.wk = ws.title
        header_offset = 2
        values = ws.get_all_values()
        collection = pd.DataFrame(values)

        if (len(collection.index) > header_offset):
            print_status("Parsing", 'Weekly Sheet | {:>3} rows'.format(len(collection)), ws.title)

            timestamps = collection.iloc[header_offset:, 0]
            raw_df = collection.ix[header_offset:, :].copy()
            raw_df.columns = self.schema

            tempList = []
            for ridx in timestamps.index.tolist():
                timestamp = self.weekday_to_date(collection.iloc[0,1], timestamps[ridx])
                tempList = tempList + [timestamp]
            raw_df['datetime'] = tempList
            self.df = self.df.append(raw_df,ignore_index = True)

        else:
            print_status("Skipped", 'Weekly Sheet |   0 rows', ws.title + "Wk")


    def parse_distribution(self):
        wss = self.collect_week_sheets()
        
        self.df = self.df.join(pd.DataFrame(columns=self.schema))

        for ws in wss:
            self.parse_dist_weeksheet(ws)

        self.df.datetime = pd.to_datetime(self.df.datetime)
        self.df.columns = self.export_cols

        print('\n')
        return self.df

    def parse_dist_weeksheet(self, ws):
        self.wk = ws.title
        
        header_offset = 2
        values = ws.get_all_values()
        try:
            if not any(values[2]):
                return
        except IndexError:
            return

        collection = pd.DataFrame(self.get_data_rows(values))
        if (len(collection.index) > header_offset):
            print_status("Parsing", 'Weekly Sheet | {:>3} rows'.format(len(collection)), ws.title)
        else:
            print_status("Skipped", 'Weekly Sheet |   0 rows', ws.title)

        collection.columns = collection.iloc[1].tolist()
        beneficiaries = collection.iloc[header_offset:, 1]
        timestamps = collection.iloc[header_offset:, 0]
        raw_df = collection.ix[header_offset:, self.schema]
        raw_df.columns = self.schema
        loc = len(self.df)

        for ridx, row in raw_df.iterrows():
            beneficiary = beneficiaries[ridx]
            timestamp = self.weekday_to_date(collection.iloc[0,1], timestamps[ridx])
            self.df.loc[loc, self.std_cols + self.schema] = self.standard_row(beneficiary, timestamp, row.tolist())
            loc += 1

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
 

    def collect_week_sheets(self):
        return [ws for ws in self.worksheets() if _is_week_number(ws.title)]

    def standard_row(self, donor, timestamp, std_values):
        return [self.org, self.programme, timestamp, donor] + std_values

    def create_translations_keys(self, terms):
        # TODO Support English Keys
        self.terms_to_sheet('Translations', terms)

    def create_mappings_keys(self, terms):
        # TODO Support English Keys
        self.terms_to_sheet('Mappings', terms)

    def create_units_keys(self, terms):
        # TODO Support English Keys
        self.terms_to_sheet('Units', terms)

    def get_data_rows(self, rows):
        return [row for row in rows if any(row)]

    def weekday_to_date(self, monday_date_as_str, week_day):
        monday_date = datetime.datetime.strptime(monday_date_as_str, '%Y-%m-%d')
        try:
            days_offset = self.weekdays.index(week_day.title())
        except ValueError:
            print("\n{: ^80}\n".format(" << ERROR : INVALID DAY OF WEEK >> "))
            print("\n{: ^80}\n".format(" Inspect {} {} {} WK {}, look for missing value".format(self.org, self.year, self.stage, self.wk)))
            raise SystemExit(0)
        record_date = monday_date + datetime.timedelta(days=days_offset)
        return record_date

def _is_week_number(title):
    if not title.isdigit():
        return False
    elif int(title) < 1:
        return False
    elif int(title) > 53:
        return False
    else:
        return True

def stop():
    import pdb; pdb.set_trace()
