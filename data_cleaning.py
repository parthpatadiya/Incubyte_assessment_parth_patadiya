import pandas as pd
import numpy as np


def data_cleaning(file_path):
    #Name of each columns
    column_name=["Customer Name","Customer ID","Customer Open Date","Last Consulted Date","Vaccination Type",
                "Doctor Consulted","State","Country","Date of Birth","Active Customer"]


    # Read pipe delimited data file
    # sep - the separator of data "|"
    # skiprows - it skips rows from beginning.1 used to skip header row.
    # skipfooter - it skips rows from ending.0 used because there is not any footer row in the end. if footer is avaible then use 1.
    # usecols - list of index of which columns needs to be added in dataframe. here first two columns ignored because first was empty and second defines |D|.
    # names - set column names. gave list of column name respectively.
    # parse_dates - used to convert date-time data from string to date time object. gave list of columns number
    data=pd.read_csv(file_path,sep="|",skiprows=1,skipfooter=0,usecols=[2,3,4,5,6,7,8,9,10,11],names=column_name,parse_dates=[2,3,8])


    # it is important converted all datatype to string for using it in insert query of table which is dynamically formed.
    data=data.astype(str)

    #replace empty or nan in non-mandatory Date fields to the default 00000000 which will be interpreted as None in date type object of sql 
    data["Last Consulted Date"]=data["Last Consulted Date"].replace("nan","00000000")
    data["Date of Birth"]=data["Date of Birth"].replace("nan","00000000")

    #coverting "nan" string to None object.
    data[data.loc[:]=="nan"]=None

    # "Customer Name","Customer ID","Customer Open Date" are mandatory fields 
    # so if any records have missing values in that columns then 
    # we have to remove that data or we can store somewhere else.
    # here we extracted indexes of data where mandatory field containing "None" or "NaT" 
    bad_data_indx=np.concatenate((np.where(data[["Customer Name","Customer ID","Customer Open Date"]]=="NaT")[0],
                                np.where(data[["Customer Name","Customer ID","Customer Open Date"]].isna())[0]))

    # storing bad-data into pipe delimited data file so we can recover missing data later
    bad_data=data.loc[bad_data_indx,:]

    #removing bad-data from our main data so we can procceed it further.
    data.drop(index=bad_data_indx,inplace=True)

    return data,bad_data