#!/usr/bin/python
# -*- coding: UTF-8-*-
__author__ = 'io'

import datetime
import pandas as pd
import os
import os.path as op
from ..connector import GoogleSourceClient
from ..credentials import getGoogleCredentials


class CanonicalTransformer(object):

    datadir = ''

    def __init__(self, datadir=''):
        self.datadir = datadir

    def dump_year(self):
        pass

    def aggregate_years(self):
        pass

    def to_csv(self, df):
        # fn = self.datadir + '.csv'
        # df.to_csv()
        pass


class SheetToCanonical(CanonicalTransformer):
    def __init__(self, name, id, dest='data/Canonical/'):
        """
        New version of GoogleToCanonical
        """
        super(SheetToCanonical, self).__init__()

        self.client = GoogleSourceClient.connect(getGoogleCredentials())
        
        name = name.split()
        self.org = name[4]
        self.stage = name[3]
        self.year = name[5]
        
        self.csv_path = dest + self.org + '/'

        if not op.exists(self.csv_path):
            os.makedirs(self.csv_path)

        self.ss = self.client.open_by_key(id, self.stage, self.org, self.year)

    def collection_sheets_to_csv(self):
        if self.stage == 'Collection':
            self.print_sheet_header()
            df = self.ss.parse_collection()
            df.to_csv(self.csv_path + self.org + '.' + str(self.year) + '.csv',
                        encoding="utf-8", index=False, date_format='%Y-%m-%d')
        else:
            raise NotImplementedError

    def terms_sheets_to_csv(self, dest='data/Canonical/'):
        for sheet, code in self.ss.collect_terms_sheets():
            df = pd.DataFrame(sheet)
            df.to_csv(self.csv_path + self.org + '.' + code + '.csv', encoding="utf-8", index=False, header=False)

    def donors_sheets_to_csv(self, dest='data/Canonical/'):
        if self.stage == 'Collection':
            df = self.ss.parse_cover_sheet()

            if op.isfile(self.csv_path + self.org + '.donors.csv'):
                df_old = pd.read_csv(self.csv_path + self.org + '.donors.csv')
                df = pd.concat([df_old, df])
                df.drop_duplicates(subset='id', keep='last', inplace=False)

            #df.to_csv(self.csv_path + self.org + '.' + str(self.year) + '.donors.csv', encoding="utf-8", index=False)
            df.to_csv(self.csv_path + self.org + '.donors.csv', encoding="utf-8", index=False)
        else:
            raise NotImplementedError

    def beneficiary_sheets_to_csv(self, dest='data/Canonical/'):
        if self.stage == 'Distribution':
            df = self.ss.parse_cover_sheet()
            df.to_csv(self.csv_path + self.org + '.' + str(self.year) + '.beneficiary.csv', encoding="utf-8", index=False)
        else:
            self.print_sheet_header()
            raise NotImplementedError

    def distribution_sheets_to_csv(self, dest='data/Canonical/'):
        if self.stage == 'Distribution':
            self.print_sheet_header()

            df = self.ss.parse_distribution()
            df.to_csv(self.csv_path + self.org + '.' + str(self.year) + '.distribution.csv', encoding="utf-8", index=False, date_format='%Y-%m-%d')
        else:
            self.print_sheet_header()
            raise NotImplementedError

    def finance_sheets_to_csv(self, dest='data/Canonical/'):
        if self.stage == 'Processing':
            df = self.ss.parse_cover_sheet()
            df.to_csv(self.csv_path + self.org + '.' + str(self.year) + '.finance.csv', encoding="utf-8", index=False)
        else:
            raise NotImplementedError

    def processing_sheets_to_csv(self, dest='data/Canonical/'):
        if self.stage == 'Processing':
            df = self.ss.parse_processing()
            df.to_csv(self.csv_path + self.org + '.' + str(self.year) + '.processing.csv', encoding="utf-8", index=False, date_format='%Y-%m-%d')
        else:
            raise NotImplementedError

    def print_sheet_header(self):
        print '\n***', self.stage, '-', self.year, '***\n'


