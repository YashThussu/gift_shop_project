import pandas as pd
import numpy as np

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 300)


class DataCleaner:

    def __init__(self, path: str, file_name: str):
        """provide location to
        latest file and file name"""
        self.path = path
        self.file_name = file_name

    def working_dataframe(self):
        """returns the dataframe of the specified input type csv/excel"""
        if self.file_name.endswith(".csv"):
            df = pd.read_csv(self.path + '\\' + self.file_name)
            return df
        elif self.file_name.endswith(".xlsx"):
            df = pd.read_excel(self.path + '\\' + self.file_name,sheet_name='Sheet2')
            return df
        else:
            return 'wrong input file type! Please select csv or excel'
    
    def step1(self):
        """ removes Nulls from Items sold column"""
        if isinstance(self.working_dataframe(), pd.DataFrame):  # to check whether working_dataframe is returning a dataframe
            df1 = self.working_dataframe()
            df1.replace(' ',np.NaN,inplace=True)

            list_columns_drop=[x for x in df1.columns if x.startswith('Unnamed')]
            if len(list_columns_drop)>0:
                df1.drop(columns=list_columns_drop,inplace=True)
            
            series_null = pd.isna(df1['Items Sold']).loc[lambda x: x == True]
            df1_processed = df1.drop(series_null.index, axis=0)
            df1_processed.reset_index(drop=True, inplace=True)

            return df1_processed
        
        else:
            return self.working_dataframe()

    def step2(self):
        """removes rows containing Total in Items Sold columns"""
        df2 = self.step1()
        series_total = df2[df2['Items Sold'] == 'TOTAL'].index
        df2_processed = df2.drop(series_total, axis=0)
        df2_processed.reset_index(drop=True, inplace=True)

        return df2_processed

    def step3(self):
        """cleans date column of date headers"""
        df3 = self.step2()
        df3_processed = df3[df3['Date '] != 'Date ']
        df3_processed.reset_index(drop=True, inplace=True)

        return df3_processed

    def step4(self):
        """assigns null values to string inside date column"""
        df4 = self.step3()
        series_str_index = df4['Date '].apply(lambda x: type(x) == str).loc[lambda x: x == True].index
        df4.loc[series_str_index, 'Date '] = np.NaN

        return df4

    def step5(self):
        """removing items sold rows from given dataframe"""
        df5 = self.step4()

        series_items_sold = df5['Items Sold'].str.contains('Items Sold')
        series_items_sold_true = series_items_sold.loc[lambda x: x == True]
        df5_processed = df5.drop(series_items_sold_true.index, axis=0)
        df5_processed.reset_index(drop=True, inplace=True)

        return df5_processed

    def step6(self):
        """removing GRAND TOTALS from given dataframe"""

        df6 = self.step5()
        series_items_grand_total = df6['Items Sold'].str.contains('GRAND TOTAL', case=False)
        series_items_grand_total_true = series_items_grand_total.loc[lambda x: x == True]
        df6_processed = df6.drop(series_items_grand_total_true.index, axis=0)
        df6_processed.reset_index(drop=True, inplace=True)

        return df6_processed

    def step7(self):
        """adding date in null values in date column"""

        df7 = self.step6()
        value = 0
        for tuples in df7['Date '].items():
            if pd.isna(df7.iloc[tuples[0], 0]) == False:
                value = df7.iloc[tuples[0], 0]
            else:
                df7.iloc[tuples[0], 0] = value
        
        df7['Date '] = pd.to_datetime(df7['Date '])
        df7['Date '] = df7['Date '].dt.date

        df7['Category']=np.NaN
        return df7

    def datetime_to_date(self, dataframe):

        dataframe['Date '] = pd.to_datetime(dataframe['Date '])
        dataframe['Date '] = dataframe['Date '].dt.date

        return dataframe


class DataAllocation:


    def __init__(self, cat_dict):
        """ Takes input category dictionary with its allocation items"""
        self.cat_dict = cat_dict

    def allocation2(self, dataframe: pd.DataFrame):
        for key, value in self.cat_dict.items():  
            check_index_set = set(pd.isna(dataframe['Category']).loc[lambda x: x == False].index)  # create a set of indices where category is NOT NaN
            text_search_index_set = set(
                dataframe['Items Sold'].str.contains(key, case=False).loc[lambda x: x == True].index  
            )                                                                                       # create another set of indices where category IS NaN

            row_to_update = list(text_search_index_set - check_index_set)  # only update those rows which are NaN (to prevent overwriting)
            dataframe.loc[row_to_update, 'Category'] = value

        # after all dict_items have been assigned we classify others items as "Others"
        check_index_set = set(pd.isna(dataframe['Category']).loc[lambda x: x == False].index)
        text_search_index_set = set(pd.isna(dataframe['Category']).loc[lambda x: x == True].index)

        row_to_update = list(text_search_index_set - check_index_set)
        dataframe.loc[row_to_update, 'Category'] = 'Other'

        return dataframe
    
class DataMerge():


    def __init__(self,dataframe1,dataframe2):
        self.dataframe1=dataframe1
        self.dataframe2=dataframe2
    
    def date_part(self,date):
        return date.month,date.year
    
    def merge_data(self):

        month1,year1=self.date_part(self.dataframe1['Date '].unique()[-1])
        month2,year2=self.date_part(self.dataframe2['Date '].unique()[-1])

        if (year1>=year2) and (month1>month2):
            
            df_merged=pd.concat([self.dataframe2,self.dataframe1])

            return df_merged.loc[:,[x for x in list(df_merged.columns) if x not in ['Final Tally','S No.']]]
        else:
            df_merged=pd.concat([self.dataframe1,self.dataframe2])

            return df_merged.loc[:,[x for x in list(df_merged.columns) if x not in ['Final Tally','S No.']]]
        


dc1 = DataCleaner(r'C:\Users\yashu\Desktop\RGE', 'rge_22-23.xlsx')
dc2=DataCleaner(r'C:\Users\yashu\Desktop\RGE','YT_605.xlsx')

# this dictionary needs to be updated when adding new categories
dict1 = {
    'print out': 'Print Out',
    'photocopy': 'Photocopy',
    'Lamination': 'Lamination',
    'Scan':'Scan',
    'Stationery|photopaper': 'Stationery',
    'Pastel Sheet|Color Sheet|White Sheet|wrapping':'Stationery',
    'Craft': 'Craft',
    'Pokemon': 'Pokemon Cards',
    'Swim':"Swimming Items",
    'Sports': 'Sports Items',
    'Gift':'Gift Items',
    'toy': "Toys"
}

da = DataAllocation(dict1)
df1_processed=da.allocation2(dc1.step7())
df2_processed=da.allocation2(dc2.step7())

m1=DataMerge(df1_processed,df2_processed)
df_final=m1.merge_data()
print(df_final.info())
# df_prob=pd.isna(df_final['Items Sold']).loc[lambda x: x==True].count()
# print(df_prob)
df_final.to_csv(r"./one_function_call/merged-data.csv",index=False)



