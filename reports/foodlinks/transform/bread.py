import pandas as pd
import numpy as np


folder_dir = "../../../../Source/Bread/"
file_name = folder_dir + "Bread Report (new) 24122015.xlsx"

export_dir = "../../../../Data/FoodLinks-Bread/"
export_name = "foodlinks_bread.csv"

print(file_name)

xl_file = pd.ExcelFile(file_name)

dfs = {sheet_name: xl_file.parse(sheet_name)
          for sheet_name in xl_file.sheet_names}

dfs['Source'].to_csv(export_dir + export_name, encoding="utf-8", index=False, date_format='%Y-%m-%d')