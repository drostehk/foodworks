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

## Input NGO, Year



from core.drive import generate_structure

ROOT_FOLDER = 'data/Canonical/'
FRESH_FOOD_CATEGORIES = ["Vegetable", "Leafy Veg", "Ground Veg",
                         "Soy Products", "Fruit", "Bread", "Meat",
                         "Seafood", "Cooked Food", "Fresh Other"]
PACKAGED_FOOD_CATEGORIES = ["Staple", "Frozen", "Condiments",
                            "Drinks", "Milk Powder", "Packaged Other"]

#STAGES = ['collection','processing','distribution']
STAGES = ['collection']
META_FILES_PROGRAMME = ['beneficiary','finance']
META_FILES_NGO = ['donors']
#META_FILES = ['units','i18n','map']
META_FILES = ['map']

def generate_all_tableau_csv():

    for stage, ngos in generate_structure().iteritems():

        for ngo, programmes in ngos.iteritems():
            # Selective processing of NGOS
            if ngo in ['ActionHealth', 'NLPRA', 'SWA', 'FoodLink (Beneficiary)', 'FoodLink']:
                continue
            else:
                for programme, sheets in programmes.iteritems():
                    if ngo == 'PSC':
                        if programmes in ['KC', 'TM', 'WTS', 'YMT']:
                            genTableauCsv(ngo, programme)
                    else:
                        genTableauCsv(ngo, programme)

def meta_csv_to_dataframe(ngo):
    metas = {}
    for meta in META_FILES:
        df = pd.read_csv(ROOT_FOLDER + meta + '.csv')

        df = df[df.organisation_id == ngo]

        if meta == 'map':
            df = df[['category', 'canonical']]

        df = df.drop_duplicates()

        metas[meta] = df

    return metas

def generate_ngo_dataframe(ngo, programme):
    fns = available_csvs(ngo)
    df_map = map_csv_to_dataframe(ngo, fns, programme)
    return(df_map)
    #import pprint
    #pprint.pprint(df_map)


def map_csv_to_dataframe(ngo, fns, programme):
    fns_in_programme = filter(lambda fn: programme in fn, fns)

    df_map = {}

    for stage in STAGES + META_FILES_PROGRAMME + META_FILES_NGO:
        fns = filter(lambda fn: stage in fn, fns_in_programme)
        path = ROOT_FOLDER + '/' + ngo + '/'
        df_map[stage] = pd.concat([pd.read_csv(path + fn) for fn in fns])

    return df_map

def available_csvs(ngo):
    included_extenstions = ['.csv']
    file_names = [fn for fn in os.listdir(ROOT_FOLDER + ngo)
        if any(fn.endswith(ext) for ext in included_extenstions)]
    return file_names


#print(generate_ngo_dataframe('PCSS', 'General')['collection'].head())
#print(generate_ngo_dataframe('PCSS', 'General')['donors'].head())
#print()
#generate_ngo_dataframe('PSC', 'KC')
#print(available_csvs('PSC'))


#print(available_csvs('PSC'))

#print(map_csv_to_dataframe('PSC', '', 'SSP')['donors'])
#print(generate_ngo_dataframe('PSC', 'SSP')['collection'])
#generate_all_tableau_csv()

#print(meta_csv_to_dataframe('PCSS')['map'])

folder_dir = ROOT_FOLDER
fresh_food = FRESH_FOOD_CATEGORIES
package_food = PACKAGED_FOOD_CATEGORIES

def check_fresh(element):
    return element.canonical in fresh_food

def getYear(element):
    return element.datetime.year

def getMonth(element):
    return element.datetime.month

def getDay(element):
    return element.datetime.day

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
    return ([i for i, x in enumerate(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']) if x == element][0] + 1)

def getMonthDays(element, year):
    monrh_days = [31, 28, 31, 30, 31, 30, 31, 30, 30, 31, 30, 31]
    result = monrh_days[element - 1]
    if year % 4 == 0 & element == 2:
        result = 29
    return result



def genTableauCsv(ngo, program, isFoodwork = False):
    print('Exporting...' + ngo + ' - ' + program)
    ## Set file name

    #datafile_name = generate_ngo_dataframe('PCSS', 'General')['collection']
    #mapfile_name = generate_ngo_dataframe('PCSS', 'General')['map']
    #donorsfile_name = generate_ngo_dataframe('PCSS', 'General')['donors']
    '''
    distfile_name = ngo + '.' + str(year) + '.distribution.csv'
    benffile_name = ngo + '.' + str(year) + '.beneficiary.csv'
    procfile_name = ngo + '.' + str(year) + '.processing.csv'
    finfile_name = ngo + '.' + str(year) + '.finance.csv'
    '''
    df = generate_ngo_dataframe(ngo, program)['collection']
    #print(df)
    #df_map = generate_ngo_dataframe('PCSS', 'General')['map']
    df_map = meta_csv_to_dataframe(ngo)['map']
    df_donors = generate_ngo_dataframe(ngo, program)['donors']

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

    df_merge['isFresh'] = df_merge.apply(check_fresh, axis=1)
    df_merge['year'] = df_merge.apply(getYear, axis=1)
    df_merge['month'] = df_merge.apply(getMonth, axis=1)
    df_merge['day'] = df_merge.apply(getDay, axis=1)
    #df_merge = df_merge[df_merge['year'] == year]

    #print(df_merge.head(20))

    #print(df_merge.isnull()['value'])
    df_merge = df_merge[np.isfinite(df_merge['value'])]

    if(1):
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
##report_to_excel('TSWN', 2015)

def getRandomValue(element):
    return int(float(element.value) *  (1 + (random.randint(-50, 100)/100.0)))

def getRandomLat(element, base):
    return base + random.choice(np.arange(-0.0001, 0.0001, 0.00001))

def getRandomLong(element, base):
    return base + random.choice(np.arange(-0.0001, 0.0001, 0.00001))

def dummy_data():
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


def getAllMergeCsv():
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

    frame.to_csv("tableau/data/master.csv", encoding="utf-8", index=False, date_format='%Y-%m-%d')




#getAllMergeCsv()


#dummy_data()


def master_donor_address():
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


'''
ngo_list = ['TSWN', 'PSC-Kowloon City', 'PSC-SSP', 'PSC-TM', 'PSC-Wong Tai Sin', 'PSC-YTM', 'SWA', 'PCSS']
isFoodwork_list = [True, True, False, True, True, True, True, True]

ngo_list = ['TSWN', 'PSC-Kowloon City', 'PSC-SSP', 'PSC-Wong Tai Sin', 'PSC-YTM', 'SWA', 'PCSS']
isFoodwork_list = [True, True, False, True, True, True, True]

ngo_list = ngo_list = ['PSC-Kowloon City', 'PSC-SSP', 'PSC-TM', 'PSC-Wong Tai Sin', 'PSC-YTM', 'PCSS', 'SWA', 'TSWN', 'WSA']
isFoodwork_list = [True, False, True, True, True, True, True, True, True]
#ngo_list = ['SWA', 'PCSS']
#yr_list = [2014, 2015]

#ngo_list = ['WSA']
#isFoodwork_list = [False]
yr_list = [2015]

ngo_dict = {"WSA": {"collection": [2015], "isFoodwork": True},
            "TSWN": {"collection": [2013, 2014, 2015], "isFoodwork": True},
            "Evergreen": {"collection": [2015], "isFoodwork": True},
            "SWA": {"collection": [2015], "isFoodwork": True},
            "Action Health": {"collection": [], "isFoodwork": True},
            "PCSS": {"collection": [2015], "isFoodwork": True},
            "PSC-SSP": {"collection": [2014, 2015], "isFoodwork": False},
            "PSC-Kowloon City": {"collection": [2014, 2015], "isFoodwork": True},
            "PSC-TM": {"collection": [2014, 2015], "isFoodwork": True},
            "PSC-YTM": {"collection": [2014, 2015], "isFoodwork": True},
            "PSC-Wong Tai Sin": {"collection": [2014, 2015], "isFoodwork": True}
           }

for ngo in ngo_dict:
    if len(ngo_dict[ngo]["collection"]) > 0:
        for yr in ngo_dict[ngo]["collection"]:
            print('=' * 40)
            print(ngo + ' ' + str(yr))
            print('=' * 40)
            genTableauCsv(ngo, yr, isFoodwork = False)

ngo_list = ["TSWN"]
yr_list = [2013, 2014, 2015]
for ngo in ngo_list:
    for yr in yr_list:
        print('=' * 40)
        print(ngo + ' ' + str(yr))
        print('=' * 40)

        #genTableauCsv(ngo, yr, isFoodwork = isFoodwork_list[ngo_list.index(ngo)])
        genTableauCsv(ngo, yr, isFoodwork = False)
'''

#genTableauCsv


#getAllMergeCsv()
#master_donor_address()


#print(generate_structure())

generate_all_tableau_csv()
getAllMergeCsv()