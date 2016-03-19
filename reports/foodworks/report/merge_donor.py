import sys
import pandas as pd

reload(sys)
sys.setdefaultencoding("utf-8")

folder_dir = "../../../../Data/Canonical/"
ngo = "TSWN"

df_d1 = pd.read_csv(folder_dir + ngo + '/TSWN.2013.donors.csv')
df_d2 = pd.read_csv(folder_dir + ngo + '/TSWN.2014.donors.csv')
df_d3 = pd.read_csv(folder_dir + ngo + '/TSWN.2015.donors.csv')

df = pd.concat([df_d1, df_d2, df_d3])
df.drop_duplicates(subset='id', keep='last', inplace=False)

df.to_csv(folder_dir + ngo + '/TSWN.donors.csv', encoding="utf-8", index=False, date_format='%Y-%m-%d')

print(len(df))

print(df.head(5))