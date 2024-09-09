import pandas as pd
import xlrd
import xlrd.sheet
Sales_dataframe= pd.read_excel(r'sample_data\Sample - Superstore.xls')
State_dataframes= pd.read_excel(r'sample_data\abberrevation.xls')
#State_dataframes['state_code'] = Sales_dataframe['State'].str[:3]
Sales_dataframe['Price Per Item'] = Sales_dataframe['Sales']/Sales_dataframe['Quantity']
print(Sales_dataframe)
new_df = pd.merge(Sales_dataframe,State_dataframes,on='State',how="left")
print(new_df)
new_df.to_csv(r'output_data\Sales_output.csv', index=True)
Sales_dataframe['Shipment Days']= Sales_dataframe['Ship Date']-Sales_dataframe['Order Date']
print(Sales_dataframe)
new_df = pd.merge(Sales_dataframe,new_df,on='State',how="left")
new_df.to_csv(r'output_data\Sales_output.csv', index=True)