import data_cleaning as dc
import sql_crud 


file_path="data.csv"

data,bad_data=dc.data_cleaning(file_path)
bad_data.to_csv("bad_data.csv",sep="|")

#credentials
sql_crud.db_name="incubyte"
sql_crud.user_name="root"
sql_crud.password="12345"


# Common Table Fields 
#Created list of table details as per mentioned in instruction
columns_name=["Customer_Name","Customer_ID","Customer_Open_Date","Last_Consulted_Date","Vaccination_Type",
             "Doctor_Consulted","State","Country","Date_of_Birth","Active_Customer"]

column_spec=["VARCHAR(255) NOT NULL PRIMARY KEY","VARCHAR(18) NOT NULL","DATE NOT NULL","DATE","VARCHAR(5)",
             "VARCHAR(255)","VARCHAR(5)","VARCHAR(5)","DATE","VARCHAR(1)"]


#list of unique country to create table accordingly
countries=data["Country"].unique()

#country-wise  data slicing and storing to corresponding table
for country in countries: 
    #create_table if not exist
    sql_crud.create_table(country,columns_name,column_spec)
    
    #extract data of particular country
    country_data=data[data["Country"]==country]
    #convert to list of records from dataframe
    records=list(country_data.to_numpy())
    
    #Insert data into corresponding table
    sql_crud.insert_records(country,records)
else:
    print("All Records are inserted Successfully")
    
#list of tables
tables=sql_crud.table_list()
print("table list : ",tables)

#show table
table_name=input("Show Table\nEnter table name: ")
if table_name in tables:
    t_data=sql_crud.Show_Table(table_name)
    print(t_data)



