#!/usr/bin/python
# -*- coding: UTF-8-*-
__author__ = 'io'

import os
import datetime
import pandas as pd
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
        self.dest   = dest

        name        = name.split()
        self.org    = name[4]
        self.stage  = name[3]
        self.year   = name[5]


        self.programme = self.set_programme(name)
        
        self.csv_path = self.dest + self.org + '/'

        if not op.exists(self.csv_path):
            os.makedirs(self.csv_path)

        self.ss = self.client.open_by_key(id, self.stage, self.org, self.year)

    # Collection Sheets to CSV

    def collection_sheets_to_csv(self):
        if self.stage == 'Collection':
            
            self.print_sheet_header()
            
            df = self.ss.parse_collection()

            csv_dest = self.csv_filename()

            self.df_to_csv(df, csv_dest)

        else:
            raise NotImplementedError

    def donors_sheets_to_csv(self):
        
        if self.stage == 'Collection':
                       
            df = self.ss.parse_cover_sheet()

            csv_dest = self.csv_filename('donors', False)

            df = self.join_if_existing_csv(df, csv_dest) 

            self.df_to_csv(df, csv_dest)

        else:
            raise NotImplementedError

    # Processing Sheets to CSV

    def finance_sheets_to_csv(self):
        if self.stage == 'Processing':
            
            self.print_sheet_header()
            
            df = self.ss.parse_cover_sheet()

            csv_dest = self.csv_filename('finance')

            self.df_to_csv(df, csv_dest)
        else:
            raise NotImplementedError

    def processing_sheets_to_csv(self):
        if self.stage == 'Processing':
            
            df = self.ss.parse_processing()

            csv_dest = self.csv_filename()

            self.df_to_csv(df, csv_dest)
        else:
            raise NotImplementedError


    # Distribution Sheets to CSV
    

    def beneficiary_sheets_to_csv(self):
        if self.stage == 'Distribution':
            df = self.ss.parse_cover_sheet()

            csv_dest = self.csv_filename('beneficiary')

            self.df_to_csv(df, csv_dest)
        else:
            raise NotImplementedError


    def distribution_sheets_to_csv(self):
        if self.stage == 'Distribution':
            
            self.print_sheet_header()
            
            df = self.ss.parse_distribution()

            csv_dest = self.csv_filename()

            self.df_to_csv(df, csv_dest)
        else:
            self.print_sheet_header()
            raise NotImplementedError


    # Meta Sheets to CSV

    def terms_sheets_to_csv(self):
        for sheet, code in self.ss.collect_terms_sheets():
            df = pd.DataFrame(sheet)
            df.to_csv(self.dest + code + '.csv', encoding="utf-8", index=False, header=False)


    # Utility Functions

    def csv_filename(self, meta="", year=True):

        csv_base = self.csv_path + self.org

        if year:
            csv_base = csv_base + '.' + str(self.year)

        if meta:
            name = meta
        else:
            name = self.stage.lower()
        
        csv_base = csv_base + '.' + name

        if self.programme:
            programme = self.programme
        else:
            programme = 'General'
            
        csv_dest = "{} - {}.csv".format(csv_base, programme)
                
        return csv_dest


    def df_to_csv(self, df, csv_dest):
        df.to_csv(csv_dest, encoding="utf-8", index=False, date_format='%Y-%m-%d')


    def set_programme(self, name):

        # FoodWorks Data - Processing PSC 2015 - SSP
        if len(name) > 6:
            return " ".join(name[7:])
        else:
            return None


    def join_if_existing_csv(self, df, csv_dest):
        if op.isfile(csv_dest):
            df_old = pd.read_csv(csv_dest)
            df = pd.concat([df_old, df])
            df.drop_duplicates(subset='id', keep='last', inplace=True)
        return df
        

    def print_sheet_header(self):
        print '\n***', self.stage, '-', self.year, '***\n'
