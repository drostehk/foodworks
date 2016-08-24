import pandas as pd
import numpy as np
import operator as op
import os
import sys
import datetime
from os import listdir
from os.path import isfile, join
import random
import glob

import requests

sys.path.append( os.path.dirname(os.path.dirname(
    os.path.dirname( os.path.abspath(__file__) ) ) ) )

base_url = 'https://maps.googleapis.com/maps/api/geocode/json?address='
api_key = '&key=AIzaSyDPlO9q1rIqfQJA3yGVoRizaCB_84CS-lc'

file_num = 2

class TableauReport(object):
    """Prepare data for Tableau
    """

    def __init__(self, skip=None):
        self.skip = skip
        self.ROOT_FOLDER = 'data/Canonical/'
        self.FRESH_FOOD_CATEGORIES = ["Vegetable", "Leafy Veg", "Ground Veg",
                                 "Soy Products", "Fruit", "Bread", "Meat",
                                 "Seafood", "Cooked Food", "Fresh Other"]
        self.PACKAGED_FOOD_CATEGORIES = ["Staple", "Frozen", "Condiments",
                                    "Drinks", "Milk Powder", "Packaged Other"]

        # STAGES = ['collection','processing','distribution']
        self.STAGES = ['collection']
        self.META_FILES_PROGRAMME = ['beneficiary', 'finance']
        self.META_FILES_NGO = ['donors']
        # META_FILES = ['units','i18n','map']
        self.META_FILES = ['map']

    def generate_all_tableau_csv(self):

        from core.drive import generate_structure

        for ngo, programmes in generate_structure()['collection'].iteritems():

            # Selective processing of NGOS
            if ngo in ['ActionHealth', 'NLPRA', 'SWA', 'FoodLink (Beneficiary)', 'FoodLink'] or ngo in self.skip:
                continue
            else:
                for programme, sheets in programmes.iteritems():
                    if ngo == 'PSC':
                        if programme in ['KC', 'TM', 'WTS', 'YTM']:
                            self.genTableauCsv(ngo, programme)
                    else:
                        self.genTableauCsv(ngo, programme)

    def meta_csv_to_dataframe(self, ngo):
        metas = {}
        for meta in self.META_FILES:
            df = pd.read_csv(self.ROOT_FOLDER + meta + '.csv')

            df = df[df.organisation_id == ngo]

            if meta == 'map':
                df = df[['category', 'canonical']]

            df = df.drop_duplicates()

            metas[meta] = df

        return metas

    def generate_ngo_dataframe(self, ngo, programme):
        fns = self.available_csvs(ngo)
        df_map = self.map_csv_to_dataframe(ngo, fns, programme)
        return(df_map)


    def map_csv_to_dataframe(self, ngo, fns, programme):
        fns_in_programme = filter(lambda fn: programme in fn, fns)

        df_map = {}

        for stage in self.STAGES + self.META_FILES_PROGRAMME + self.META_FILES_NGO:
            fns = filter(lambda fn: stage in fn, fns_in_programme)
            path = self.ROOT_FOLDER + '/' + ngo + '/'
            df_map[stage] = pd.concat([pd.read_csv(path + fn) for fn in fns])

        return df_map

    def available_csvs(self, ngo):
        included_extenstions = ['.csv']
        file_names = [fn for fn in os.listdir(self.ROOT_FOLDER + ngo)
            if any(fn.endswith(ext) for ext in included_extenstions)]
        return file_names

    # Helper Methods

    def check_fresh(self, element):
        return element.canonical in self.FRESH_FOOD_CATEGORIES

    def getYear(self, element):
        return element.datetime.year

    def getMonth(self, element):
        return element.datetime.month

    def getDay(self, element):
        return element.datetime.day

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
        return ([i for i, x in enumerate(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']) if x == element][0] + 1)

    def getMonthDays(self, element, year):
        monrh_days = [31, 28, 31, 30, 31, 30, 31, 30, 30, 31, 30, 31]
        result = monrh_days[element - 1]
        if year % 4 == 0 & element == 2:
            result = 29
        return result



    def genTableauCsv(self, ngo, program, isFoodwork = False):
        print('Exporting...' + ngo + ' - ' + program)
        ## Set file name

        #datafile_name = self.generate_ngo_dataframe('PCSS', 'General')['collection']
        #mapfile_name = self.generate_ngo_dataframe('PCSS', 'General')['map']
        #donorsfile_name = self.generate_ngo_dataframe('PCSS', 'General')['donors']
        '''
        distfile_name = ngo + '.' + str(year) + '.distribution.csv'
        benffile_name = ngo + '.' + str(year) + '.beneficiary.csv'
        procfile_name = ngo + '.' + str(year) + '.processing.csv'
        finfile_name = ngo + '.' + str(year) + '.finance.csv'
        '''
        df = self.generate_ngo_dataframe(ngo, program)['collection']
        df = df.drop_duplicates()
        #print(df)
        #df_map = generate_ngo_dataframe('PCSS', 'General')['map']
        df_map = self.meta_csv_to_dataframe(ngo)['map']
        df_donors = self.generate_ngo_dataframe(ngo, program)['donors']

        ## Collection
        # Reshape the dataframe
        melt_head = ['datetime', 'donor', 'organisation_id', 'programme']
        rest_col = [x for x in list(df.columns.values) if x not in melt_head]

        df = pd.melt(df, id_vars=melt_head, value_vars=rest_col).drop_duplicates()
        df['datetime'] = pd.to_datetime(df['datetime'])

        df = df[~df[['value']].isnull().any(axis=1)]

        #df_map = df_map[df_map.organisation_id == ngo]

        #df_map = df_map[['category', 'canonical']]
        #df_map = df_map.drop_duplicates()
        df_merge = pd.merge(df, df_map, how='left', left_on=['variable'], right_on=['category'])
        df_merge = df_merge.drop('category', 1)

        df_donors = df_donors[['id', 'name_zh', 'category', 'location']]
        df_donors.rename(columns={'name_zh': 'name_zh', 'category': 'donor_category'}, inplace=True)
        df_merge = pd.merge(df_merge, df_donors, how='left', left_on=['donor'], right_on=['id'])
        df_merge = df_merge.drop('id', 1)
        df_merge['donor_category'] = df_merge['donor_category'].astype(basestring)

        df_merge['isFresh'] = df_merge.apply(self.check_fresh, axis=1)
        df_merge['year'] = df_merge.apply(self.getYear, axis=1)
        df_merge['month'] = df_merge.apply(self.getMonth, axis=1)
        df_merge['day'] = df_merge.apply(self.getDay, axis=1)
        #df_merge = df_merge[df_merge['year'] == year]

        #print(df_merge.head(20))

        #print(df_merge.isnull()['value'])
        df_merge = df_merge[np.isfinite(df_merge['value'])]

        if True:
            dest = 'tableau/data/'

            if not os.path.exists(dest):
                os.makedirs(dest)

            ## Export to Excel
            file_dir = dest + ngo + '.' + program + '.merge.csv'
            df_merge.to_csv(file_dir, encoding="utf-8", index=False, date_format='%Y-%m-%d')


        #return(df_report.fillna(0))

    '''
    def report_to_excel(ngo, year, dest='../../../../Tableau/Data/'):
        file_dir = dest + ngo + '.' + str(year) + '.report.xlsx'
        print('Report generating to: ' + file_dir)
        genReport(ngo, year).to_excel(file_dir, index_label='label', merge_cells=False, sheet_name = ngo + '.' + str(year))
        print('Done!')
    '''

    def getRandomValue(self, element):
        return int(float(element.value) *  (1 + (random.randint(-50, 100)/100.0)))

    def getRandomLat(self, element, base):
        return base + random.choice(np.arange(-0.0001, 0.0001, 0.00001))

    def getRandomLong(self, element, base):
        return base + random.choice(np.arange(-0.0001, 0.0001, 0.00001))

    def dummy_data(self):
        dest='../../../../Tableau/Data/'
        all_merge = [ f for f in listdir(dest) if (isfile(join(dest,f)) & f.endswith(".merge.csv") ) ]
        df = pd.read_csv(dest + all_merge[1])
        df_dummy = df
        df_dummy.organisation_id = ["Dummy_NGO_1"] * len(df)

        getLat = lambda x: getRandomLat(x, 22.2783)
        getLong = lambda x: getRandomLong(x, 114.2033843)

        df_dummy['lat'] = df_dummy['organisation_id'].map(getLat)
        df_dummy['lat'] = df_dummy['organisation_id'].map(getLat)
        list_lat = np.arange(22, 23, 0.2)
        list_long = np.arange(113.2, 115.2, 0.2)
        for num in range(2, 11):
            df_temp = df
            df_temp.organisation_id = ["Dummy_NGO_" + str(num)] * len(df)
            df_temp.value = df_temp.apply(getRandomValue, axis=1)

            getLat = lambda x: getRandomLat(x, random.choice(list_lat))
            getLong = lambda x: getRandomLong(x, random.choice(list_long))

            df_dummy['lat'] = df_dummy['organisation_id'].map(getLat)
            df_dummy['long'] = df_dummy['organisation_id'].map(getLong)



        df_dummy.to_csv(dest + "master.merge.csv", encoding="utf-8", index=False, date_format='%Y-%m-%d')


    def getAllMergeCsv(self):
        file_list = (glob.glob('tableau/data/*.merge.csv'))

        print('Merging ' + str(len(file_list)) + ' files...')

        frame = pd.DataFrame()
        list_ = []
        for file_ in file_list:
            df = pd.read_csv(file_,index_col=None)
            list_.append(df)
        frame = pd.concat(list_, ignore_index = True)

        # Extract NGO name
        s_ngo = lambda x: x.split("-")[0]
        frame['ngo'] = frame.organisation_id.apply(s_ngo)
        frame = frame.drop_duplicates()

        frame.to_csv("tableau/data/master.csv", encoding="utf-8", index=False, date_format='%Y-%m-%d')


    def master_donor_address(self):
        df = pd.read_csv('../../../../Tableau/Data/master.merge.csv')

        df_loc = df[['location']]

        unique_loc = df_loc.iloc[:,0].unique()

        formatted_name = list()
        district = list()
        territories = list()
        country = list()
        lat = list()
        lng = list()
        formatted_address = list()

        for address in unique_loc:
            address = str(address)
            the_url = base_url + address.replace(' ', '+') + ',Hong+Kong' + api_key
            response = requests.get(the_url)
            json_dict = response.json()
            if json_dict['status'] == 'OVER_QUERY_LIMIT':
                print('Over Limit!')
            else:
                if json_dict['status'] == 'ZERO_RESULTS':
                    formatted_name += [u'ZERO_RESULTS']
                    district += [u'ZERO_RESULTS']
                    territories += [u'ZERO_RESULTS']
                    country += [u'ZERO_RESULTS']
                    lat += [None]
                    lng += [None]
                    formatted_address += [u'ZERO_RESULTS']
                else:
                    for number in range(0,len(json_dict['results'][0]['address_components'])):
                        if 'point_of_interest' in json_dict['results'][0]['address_components'][number]['types']:
                            formatted_name += [json_dict['results'][0]['address_components'][number]['long_name']]
                        if 'neighborhood' in json_dict['results'][0]['address_components'][number]['types']:
                            district += [json_dict['results'][0]['address_components'][number]['long_name']]
                        if 'administrative_area_level_1' in json_dict['results'][0]['address_components'][number]['types']:
                            territories += [json_dict['results'][0]['address_components'][number]['long_name']]
                        if 'country' in json_dict['results'][0]['address_components'][number]['types']:
                            country += [json_dict['results'][0]['address_components'][number]['long_name']]

                    lat += [json_dict['results'][0]['geometry']['location']['lat']]
                    lng += [json_dict['results'][0]['geometry']['location']['lng']]
                    formatted_address += [json_dict['results'][0]['formatted_address']]
                    if len(formatted_name) != len(formatted_address):
                        formatted_name += [None]
                    if len(district) != len(formatted_address):
                        district += [None]
                    if len(territories) != len(formatted_address):
                        territories += [None]
                    if len(country) != len(formatted_address):
                        country += [None]
        unique_loc = pd.DataFrame(unique_loc)
        unique_loc.columns = ['location']

        unique_loc['name_eng'] = formatted_name
        unique_loc['district'] = district
        unique_loc['territories'] = territories
        unique_loc['country'] = country
        unique_loc['lat'] = lat
        unique_loc['lng'] = lng
        unique_loc['formatted_add_eng'] = formatted_address

        df = pd.merge(df, unique_loc, how='left', left_on=['location'], right_on=['location'])
        df.to_csv("../../../../Tableau/Data/master.merge.csv", encoding="utf-8", index=False, date_format='%Y-%m-%d')


if __name__ == '__main__':
    generate_all_tableau_csv()
    getAllMergeCsv()
