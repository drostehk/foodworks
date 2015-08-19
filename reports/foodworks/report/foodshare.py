import pandas as pd
import numpy as np
import operator as op
import os
file_num = 2

## Input NGO, Year

folder_dir = "../../../../Data/Canonical/"
fresh_food = ["Vegitable", "Leafy Veg", "Ground Veg", "Soy Products", "Fruit", "Bread", "Meat", "Seafood", "Cooked Food", "Fresh Other"]
package_food = ["Staple", "Frozen", "Condiment", "Drinks", "Milk Powder", "Packaged Other"]

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

def genReport(ngo, year):
	datafile_name = ngo + '.' + str(year) + '.csv'
	mapfile_name = ngo + '.map.csv'
	donorsfile_name = ngo + '.donors.csv'
	distfile_name = ngo + '.' + str(year) + '.distribution.csv'
	benffile_name = ngo + '.beneficiary.csv'
	procfile_name = ngo + '.' + str(year) + '.processing.csv'
	finfile_name = ngo + '.' + str(year) + '.finance.csv'

	# Load the data in
	df = pd.read_csv(folder_dir + ngo + '/' + datafile_name)
	df_map = pd.read_csv(folder_dir + ngo + '/' + mapfile_name)
	df_donors = pd.read_csv(folder_dir + ngo + '/' + donorsfile_name)
	df_dist = pd.read_csv(folder_dir + ngo + '/' + distfile_name)
	df_benf = pd.read_csv(folder_dir + ngo + '/' + benffile_name)
	df_proc = pd.read_csv(folder_dir + ngo + '/' + procfile_name)
	df_fin = pd.read_csv(folder_dir + ngo + '/' + finfile_name)

	if os.path.isfile(folder_dir + ngo + '/' + ngo + '.' + str(year + 1) + '.csv'):
		df = pd.concat([df, pd.read_csv(folder_dir + ngo + '/' + ngo + '.' + str(year + 1) + '.csv')])

	if os.path.isfile(folder_dir + ngo + '/' + ngo + '.' + str(year + 1) + '.csv'):
		df_dist = pd.concat([df_dist, pd.read_csv(folder_dir + ngo + '/' + ngo + '.' + str(year + 1) + '.distribution.csv')])

	if os.path.isfile(folder_dir + ngo + '/' + ngo + '.' + str(year + 1) + '.csv'):
		df_proc = pd.concat([df_proc, pd.read_csv(folder_dir + ngo + '/' + ngo + '.' + str(year + 1) + '.processing.csv')])
	
	## Collection
	# Reshape the dataframe
	df = pd.melt(df, id_vars=list(df.columns.values)[0:4], value_vars=list(df.columns.values)[4:])
	df['datetime'] = pd.to_datetime(df['datetime'])

	df_map = df_map[['category', 'canonical']]
	df_merge = pd.merge(df, df_map, how='left', left_on=['variable'], right_on=['category'])
	df_merge = df_merge.drop('category', 1)

	df_donors = df_donors[['name_zh', 'category']]
	df_donors.rename(columns={'name_zh': 'name_zh', 'category': 'donor_category'}, inplace=True)
	df_merge = pd.merge(df_merge, df_donors, how='left', left_on=['donor'], right_on=['name_zh'])
	df_merge = df_merge.drop('name_zh', 1)
	df_merge['donor_category'] = df_merge['donor_category'].astype(basestring)

	df_merge['isFresh'] = df_merge.apply(check_fresh, axis=1)
	df_merge['year'] = df_merge.apply(getYear, axis=1)
	df_merge['month'] = df_merge.apply(getMonth, axis=1)
	df_merge['day'] = df_merge.apply(getDay, axis=1)
	df_merge = df_merge[df_merge['year'] == year]

	## Distribution
	df_dist['datetime'] = pd.to_datetime(df_dist['datetime'])
	df_dist['year'] = df_dist.apply(getYear, axis=1)
	df_dist['month'] = df_dist.apply(getMonth, axis=1)
	df_dist['day'] = df_dist.apply(getDay, axis=1)
	df_dist = df_dist[df_dist['year'] == year]
	## TODO: Need to check for pervious/next year file

	## Processing
	df_proc['datetime'] = pd.to_datetime(df_proc['datetime'])
	df_proc['year'] = df_proc.apply(getYear, axis=1)
	df_proc['month'] = df_proc.apply(getMonth, axis=1)
	df_proc['day'] = df_proc.apply(getDay, axis=1)
	df_proc = df_proc[df_proc['year'] == year]

	df_fin.columns = ['month', 'income', 'expenditure']
	df_fin['month_num'] = (df_fin.index + 1)

	# Empty DF for report
	columns = ['element'] + map(str, range(1,13))
	df_report = pd.DataFrame(columns=columns)

	df_report.loc[len(df_report)+1] = genRow('Total volume of food collected (kg)', df_merge.groupby('month').value.agg(['sum'])['sum'])
	df_report.loc[len(df_report)+1] = genRow('Total volume of fresh food (kg)', df_merge[df_merge['isFresh'] == True].groupby('month').value.agg(['sum'])['sum'])
	df_report.loc[len(df_report)+1] = genRow('Total volume of packaged food (kg)', df_merge[df_merge['isFresh'] == False].groupby('month').value.agg(['sum'])['sum'])


	df_report.loc[len(df_report)+1] = genRow('Number of food rescue days/ month', df_merge.groupby('month').day.nunique())
	df_report.loc[len(df_report)+1] = genRow('Total Donors Count', df_merge.groupby('month').donor.agg(['count'])['count'])
	df_report.loc[len(df_report)+1] = genRow('Unique Donors Count', df_merge.groupby('month').donor.nunique())
	
	for donor in pd.unique(df_merge['donor_category']):
		df_report.loc[len(df_report)+1] = genRow(donor.title() + ' (kg)', df_merge[df_merge['donor_category'] == donor].groupby('month').value.agg(['sum'])['sum'])
	df_report.loc[len(df_report)+1] = genRow('Total beneficiaries', df_dist.groupby('month').Distribution_Count.agg(['sum'])['sum'])

	df_report.loc[len(df_report)+1] = genRow('Compost volume (kg)', df_proc.groupby('month').compost.agg(['sum'])['sum'])
	df_report.loc[len(df_report)+1] = genRow('Disposal volume (kg)', df_proc.groupby('month').disposal.agg(['sum'])['sum'])
	df_report.loc[len(df_report)+1] = genRow('Storage volume (kg)', df_proc.groupby('month').storage.agg(['sum'])['sum'])

	df_report.loc[len(df_report)+1] = genRow('Donation/ Income ($)', df_fin.groupby('month').income.agg(['sum'])['sum'])
	df_report.loc[len(df_report)+1] = genRow('Total expenditure ($)', df_fin.groupby('month').income.agg(['sum'])['sum'])

	df_report.loc[len(df_report)+1] = ['Total distribution volume (kg)'] + [a - e - f + g for a, e, f, g in zip(getList(df_report, 'Total volume of food collected (kg)'), getList(df_report, 'Compost volume (kg)'), getList(df_report, 'Disposal volume (kg)'), getList(df_report, 'Storage volume (kg)'))]	
	df_report.loc[len(df_report)+1] = ['Percentage of food distributed for consumption (%)'] + [d / a for d, a in zip(getList(df_report, 'Total distribution volume (kg)'), getList(df_report, 'Total volume of food collected (kg)'))]
	df_report.loc[len(df_report)+1] = ['Compost Percentage (%)'] + [e / a for e, a in zip(getList(df_report, 'Compost volume (kg)'), getList(df_report, 'Total volume of food collected (kg)'))]
	df_report.loc[len(df_report)+1] = ['Disposal Percentrage (%)'] + [f / a for f, a in zip(getList(df_report, 'Disposal volume (kg)'), getList(df_report, 'Total volume of food collected (kg)'))]
	df_report.loc[len(df_report)+1] = ['Storage Percentage (%)'] + [g / a for g, a in zip(getList(df_report, 'Storage volume (kg)'), getList(df_report, 'Total volume of food collected (kg)'))]
	df_report.loc[len(df_report)+1] = ['Average amount of food rescued/ day (kg)'] + [a / h for a , h in zip(getList(df_report, 'Total volume of food collected (kg)'), getList(df_report, 'Number of food rescue days/ month'))]
	df_report.loc[len(df_report)+1] = ['Average beneficiaries/day '] + [a / h for a , h in zip(getList(df_report, 'Total beneficiaries'), getList(df_report, 'Number of food rescue days/ month'))]
	df_report.loc[len(df_report)+1] = ['Average volume of food distributed/ per person  / day (kg)'] + [i / d / h for i , d, h in zip(getList(df_report, 'Total distribution volume (kg)'), getList(df_report, 'Number of food rescue days/ month'), getList(df_report, 'Total beneficiaries'))]
	df_report.loc[len(df_report)+1] = ['Average cost/ beneficiary ($)'] + [k / i for k , i in zip(getList(df_report, 'Total expenditure ($)'), getList(df_report, 'Total beneficiaries'))]

	df_report.loc[len(df_report)+1] = ['Average cost/kg of rescued food ($)'] + [k / i for k , i in zip(getList(df_report, 'Total expenditure ($)'), getList(df_report, 'Total volume of food collected (kg)'))]
	df_report.loc[len(df_report)+1] = ['Average cost/ kg of distributed food ($)'] + [k / i for k , i in zip(getList(df_report, 'Total expenditure ($)'), getList(df_report, 'Total distribution volume (kg)'))]

	return(df_report)

print(genReport('TSWN', 2014))
#genReport('TSWN', 2015)

