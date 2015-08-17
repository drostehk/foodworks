import pandas as pd
import numpy as np

import os
file_num = 2

## Input NGO, Year

#/Users/DicksonK/Google Drive/Project/FoodWorks/Code/reports/foodworks/report
folder_dir = "../../../../Data/Canonical/"
fresh_food = ["Vegitable", "Leafy Veg", "Ground Veg", "Soy Products", "Fruit", "Bread", "Meat", "Seafood", "Cooked Food", "Fresh Other"]
package_food = ["Staple", "Frozen", "Condiment", "Drinks", "Milk Powder", "Packaged Other"]

def check_fresh(element):
	return element.canonical in fresh_food

def getMonth(element):
	return element.datetime.month

def getDay(element):
	return element.datetime.day

def genReport(ngo, year):
	datafile_name = ngo + '.' + str(year) + '.csv'
	mapfile_name = ngo + '.map.csv'
	donorsfile_name = ngo + '.donors.csv'

	# Load the data in
	df = pd.read_csv(folder_dir + ngo + '/' + datafile_name)
	df_map = pd.read_csv(folder_dir + ngo + '/' + mapfile_name)
	df_donors = pd.read_csv(folder_dir + ngo + '/' + donorsfile_name)

	# Reshape the dataframe
	df = pd.melt(df, id_vars=list(df.columns.values)[0:4], value_vars=list(df.columns.values)[4:])
	df['datetime'] = pd.to_datetime(df['datetime'])

	df_map = df_map[['category', 'canonical']]
	
	df_merge = pd.merge(df, df_map, how='left', left_on=['variable'], right_on=['category'])

	df_merge = df_merge.drop('category', 1)

	df_merge['isFresh'] = df_merge.apply(check_fresh, axis=1)
	df_merge['month'] = df_merge.apply(getMonth, axis=1)
	df_merge['day'] = df_merge.apply(getDay, axis=1)
	
	df_merge_fresh = df_merge[df_merge['isFresh'] == True]

	df_merge_nonfresh = df_merge[df_merge['isFresh'] == False]

	
	print(df_merge.head())
	print(df_donors.head())

	print(df_merge.groupby('month').value.agg(['sum']))
	print(df_merge_fresh.groupby('month').value.agg(['sum']))
	print(df_merge_nonfresh.groupby('month').value.agg(['sum']))

	print(df_merge.groupby('month').day.nunique())

	print(df_merge.groupby('month').donor.nunique())
	print(df_merge.groupby('month').donor.agg(['count']))
	
	
	#print df_merge.dtypes
	#print df_merge.head()

	#print df.head()
	#print df_merge.head()




genReport('TSWN', 2014)
#genReport('TSWN', 2015)

