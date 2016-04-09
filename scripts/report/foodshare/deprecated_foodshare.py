# def genReport(ngo, year):
	# datafile_name = ngo + '.' + str(year) + '.csv'
	# mapfile_name = ngo + '.map.csv'
	# donorsfile_name = ngo + '.donors.csv'
	# distfile_name = ngo + '.' + str(year) + '.distribution.csv'
	# benffile_name = ngo + '.' + str(year) + '.beneficiary.csv'
	# procfile_name = ngo + '.' + str(year) + '.processing.csv'
	# finfile_name =  ngo + '.' + str(year) + '.finance.csv'

	# # Load the data in
	# df = pd.read_csv(folder_dir + ngo + '/' + datafile_name)
	# df_map = pd.read_csv(folder_dir + ngo + '/' + mapfile_name)
	# df_donors = pd.read_csv(folder_dir + ngo + '/' + donorsfile_name)
	# df_dist = pd.read_csv(folder_dir + ngo + '/' + distfile_name)
	# df_benf = pd.read_csv(folder_dir + ngo + '/' + benffile_name)
	# df_proc = pd.read_csv(folder_dir + ngo + '/' + procfile_name)
	# df_fin = pd.read_csv(folder_dir + ngo + '/' + finfile_name)

	# yr_offset = [-1, 1]

	# for yroff in yr_offset:
	# 	if os.path.isfile(folder_dir + ngo + '/' + ngo + '.' + str(year + yroff) + '.csv'):
	# 		df = pd.concat([df, pd.read_csv(folder_dir + ngo + '/' + ngo + '.' + str(year + yroff) + '.csv')])

	# 	if os.path.isfile(folder_dir + ngo + '/' + ngo + '.' + str(year + yroff) + '.distribution.csv'):
	# 		df_dist = pd.concat([df_dist, pd.read_csv(folder_dir + ngo + '/' + ngo + '.' + str(year + yroff) + '.distribution.csv')])

	# 	if os.path.isfile(folder_dir + ngo + '/' + ngo + '.' + str(year + yroff) + '.processing.csv'):
	# 		df_proc = pd.concat([df_proc, pd.read_csv(folder_dir + ngo + '/' + ngo + '.' + str(year + yroff) + '.processing.csv')])
	
	## Collection
	# Reshape the dataframe
	# df['datetime'] = pd.to_datetime(df['datetime'])
	# df = df[df.apply(getYear, axis=1) == year]
	# df = df.fillna(0)

	# df = df_map['collection']
	# melt_head = ['datetime', 'donor', 'organisation_id', 'programme']
	# rest_col = [x for x in list(df.columns.values) if x not in melt_head]

	# df = pd.melt(df, id_vars=melt_head, value_vars=rest_col)
	# df['datetime'] = pd.to_datetime(df['datetime'])
	# df = df[df.value != 0]
	# df = df[df['value'].notnull()]

	#print(df.head(100))
	# #print(df.head(100))
	# df_map = df_map[df_map.organisation_id == ngo]
	# df_map = df_map[['category', 'canonical']]
	# df_map = df_map.drop_duplicates()

	# df_merge = pd.merge(df, df_map, how='left', left_on=['variable'], right_on=['category'])
	# df_merge = df_merge.drop('category', 1)

	# df_donors = df_donors[['id', 'foodshare_category']]
	# df_donors.rename(columns={'id': 'id', 'foodshare_category': 'donor_category'}, inplace=True)

	# #print(df_donors)

	# df_merge = pd.merge(df_merge, df_donors, how='left', left_on=['donor'], right_on=['id'])
	# df_merge = df_merge.drop('id', 1)


	# df_merge['donor_category'] = df_merge['donor_category'].astype(basestring)

	df_merge['isFresh'] = df_merge.apply(check_fresh, axis=1)
	df_merge['year'] = df_merge.apply(getYear, axis=1)
	df_merge['month'] = df_merge.apply(getMonth, axis=1)
	df_merge['day'] = df_merge.apply(getDay, axis=1)
	df_merge = df_merge[df_merge['year'] == year]

	# ## Distribution
	# df_dist['datetime'] = pd.to_datetime(df_dist['datetime'])
	# df_dist['year'] = df_dist.apply(getYear, axis=1)
	# df_dist['month'] = df_dist.apply(getMonth, axis=1)
	# df_dist['day'] = df_dist.apply(getDay, axis=1)
	# df_dist = df_dist[df_dist['year'] == year]
	## TODO: Need to check for pervious/next year file

	## Processing
	# df_proc['datetime'] = pd.to_datetime(df_proc['datetime'])
	# df_proc['year'] = df_proc.apply(getYear, axis=1)
	# df_proc['month'] = df_proc.apply(getMonth, axis=1)
	# df_proc['day'] = df_proc.apply(getDay, axis=1)
	# df_proc = df_proc[df_proc['year'] == year]


	##Fin
	# df_fin.columns = ['month', 'income', 'expenditure']
	# df_fin['month_num'] = (df_fin.index + 1)

	# df_merge = df_merge[df_merge['donor_category'].notnull()]

	# df_merge = df_merge[]

	# Empty DF for report
	# columns = ['element'] + map(str, range(1,13))
	# df_report = pd.DataFrame(columns=columns)

	# df_report.loc[len(df_report)+1] = genRow('Total volume of food collected (kg)', df_merge.groupby('month').value.agg(['sum'])['sum'])
	# df_report.loc[len(df_report)+1] = genRow('Total volume of fresh food (kg)', df_merge[df_merge['isFresh'] == True].groupby('month').value.agg(['sum'])['sum'])
	# df_report.loc[len(df_report)+1] = genRow('Total volume of packaged food (kg)', df_merge[df_merge['isFresh'] == False].groupby('month').value.agg(['sum'])['sum'])


	# df_report.loc[len(df_report)+1] = genRow('Number of food rescue days/ month', df_merge.groupby('month').day.nunique())
	# df_report.loc[len(df_report)+1] = genRow('Total Donors Count', df_merge.groupby('month').donor.agg(['count'])['count'])
	# df_report.loc[len(df_report)+1] = genRow('Unique Donors Count', df_merge.groupby('month').donor.nunique())

	# #print(pd.unique(df_merge['donor_category']))
	# #print(df_merge[df_merge['donor_category'].isnull()])


	# df_report.loc[len(df_report)+1] = genRow('Total beneficiaries', df_dist.groupby('month').Distribution_Count.agg(['sum'])['sum'])

	# df_report.loc[len(df_report)+1] = genRow('Compost volume (kg)', df_proc.groupby('month').compost.agg(['sum'])['sum'])
	# df_report.loc[len(df_report)+1] = genRow('Disposal volume (kg)', df_proc.groupby('month').disposal.agg(['sum'])['sum'])
	# df_report.loc[len(df_report)+1] = genRow('Storage volume (kg)', df_proc.groupby('month').storage.agg(['sum'])['sum'])

	# df_report.loc[len(df_report)+1] = genRow('Donation/ Income ($)', df_fin.groupby('month_num').income.agg(['sum'])['sum'])
	# df_report.loc[len(df_report)+1] = genRow('Total expenditure ($)', df_fin.groupby('month_num').expenditure.agg(['sum'])['sum'])

	# df_report = df_report.fillna(0)

	# df_report.loc[len(df_report)+1] = ['Total distribution volume (kg)'] + [a - e - f + g for a, e, f, g in zip(getList(df_report, 'Total volume of food collected (kg)'), getList(df_report, 'Compost volume (kg)'), getList(df_report, 'Disposal volume (kg)'), getList(df_report, 'Storage volume (kg)'))]
	# df_report.loc[len(df_report)+1] = ['Percentage of food distributed for consumption (%)'] + [d / a for d, a in zip(getList(df_report, 'Total distribution volume (kg)'), getList(df_report, 'Total volume of food collected (kg)'))]
	# df_report.loc[len(df_report)+1] = ['Compost Percentage (%)'] + [e / a for e, a in zip(getList(df_report, 'Compost volume (kg)'), getList(df_report, 'Total volume of food collected (kg)'))]
	# df_report.loc[len(df_report)+1] = ['Disposal Percentrage (%)'] + [f / a for f, a in zip(getList(df_report, 'Disposal volume (kg)'), getList(df_report, 'Total volume of food collected (kg)'))]
	# df_report.loc[len(df_report)+1] = ['Storage Percentage (%)'] + [g / a for g, a in zip(getList(df_report, 'Storage volume (kg)'), getList(df_report, 'Total volume of food collected (kg)'))]
	# df_report.loc[len(df_report)+1] = ['Average amount of food rescued/ day (kg)'] + [a / h for a , h in zip(getList(df_report, 'Total volume of food collected (kg)'), getList(df_report, 'Number of food rescue days/ month'))]
	# df_report.loc[len(df_report)+1] = ['Average beneficiaries/day '] + [a / h for a , h in zip(getList(df_report, 'Total beneficiaries'), getList(df_report, 'Number of food rescue days/ month'))]
	# df_report.loc[len(df_report)+1] = ['Average volume of food distributed/ per person  / day (kg)'] + [i / d / h for i , d, h in zip(getList(df_report, 'Total distribution volume (kg)'), getList(df_report, 'Number of food rescue days/ month'), getList(df_report, 'Total beneficiaries'))]
	# df_report.loc[len(df_report)+1] = ['Average cost/ beneficiary ($)'] + [k / i for k , i in zip(getList(df_report, 'Total expenditure ($)'), getList(df_report, 'Total beneficiaries'))]

	# df_report.loc[len(df_report)+1] = ['Average cost/kg of rescued food ($)'] + [k / i for k , i in zip(getList(df_report, 'Total expenditure ($)'), getList(df_report, 'Total volume of food collected (kg)'))]
	# df_report.loc[len(df_report)+1] = ['Average cost/ kg of distributed food ($)'] + [k / i for k , i in zip(getList(df_report, 'Total expenditure ($)'), getList(df_report, 'Total distribution volume (kg)'))]

	# df_report = df_report.iloc[[0, 1, 2, 12, 13, 7, 14, 8, 15, 9, 16, 3, 17, 18, 19, 10, 11, 20, 21, 22, 4, 5]].copy()

	# df_report.index = np.arange(1, len(df_report) + 1)

	# for donor in pd.unique(df_merge['donor_category']):
	# 	df_report.loc[len(df_report)+1] = genRow(donor.title() + ' (kg)', df_merge[df_merge['donor_category'] == donor].groupby('month').value.agg(['sum'])['sum'])


	# df_report['Total'] = df_report.ix[:, 1:13].sum(axis=1)
	# df_report['Average'] = df_report.ix[:, 1:13].mean(axis=1)

	# return(df_report.fillna(0))


# def report_to_excel(ngo, year, dest='data/Report/'):
# 	file_dir = dest + ngo + '.' + str(year) + '.report.xlsx'
# 	print('Report generating to: ' + file_dir)
# 	genReport(ngo, year).to_excel(file_dir, index_label='label', merge_cells=False, sheet_name = ngo + '.' + str(year))
# 	print('Done!')

# ngo_list = ['SWA', 'PCSS', 'TSWN', 'PSC-Kowloon City', 'PSC-SSP', 'PSC-TM', 'PSC-Wong Tai Sin', 'PSC-YTM']
# ngo_list = ['PCSS', 'TSWN', 'PSC-Kowloon City', 'PSC-SSP', 'PSC-TM', 'PSC-Wong Tai Sin', 'PSC-YTM']
# ngo_list = ['SWA', 'TSWN']
# ngo_list = ['PCSS']
# ngo_list = ['WSA']

# for ngo in ngo_list:
# 	report_to_excel(ngo, 2015)

# report_to_excel('TSWN', 2015)
# report_to_excel('PSC Kowloon City', 2015)

#genReport('TSWN', 2015)