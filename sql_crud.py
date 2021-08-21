import mysql.connector as mysql
import pandas as pd
import numpy as np


#credentials
user_name="root"
password="12345"
db_name="incubyte"

# Connect to Mysql Database
try:
    connection = mysql.connect(host='localhost',
                                        database=db_name,
                                        user=user_name,
                                        password=password)
    if connection.is_connected():
        cursor = connection.cursor(buffered=True)
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
except Exception as e:
    print("Error while connecting to MySQL", e)



# Create Table      

def create_table(name,columns_name,column_spec):
    cursor=connection.cursor()
    cursor.execute("SHOW TABLES FROM "+db_name)
    tables=[i[0] for i in cursor.fetchall()]
    if (name not in tables) and (len(columns_name)==len(column_spec)):
        try:
            tbl_cols=[(" ").join([i,j]) for i,j in zip(columns_name,column_spec)]
            query='CREATE TABLE  IF NOT EXISTS '+name+' (' + ', '.join(tbl_cols) + ');'
            cursor.execute(query)
            print(name+" table successfully created")
        except Exception as e:
            print("Error while creating table: "+e)
    else:
        if (len(columns_name)!=len(column_spec)):
            print("number of Column names and column specifications not same.")
        else:
            print("Table already exists")
    cursor.close()


# Insert  Records in Table

#list of tuples for each records
def insert_records(table_nm,records):
    try:
        cursor=connection.cursor()
        qry="insert into "+table_nm+" VALUES(%s, %s ,%s ,%s ,%s ,%s ,%s ,%s , STR_TO_DATE(%s, '%d%m%Y') ,%s);"
        cursor.executemany(qry,records)
        cursor.close()
    except Exception as e:
            print("Error while insert records : "+str(e))
    


# Show Mysql Table data in DataFrame

def Show_Table(table_name):
    cursor=connection.cursor()
    cursor.execute("select * from "+table_name)
    read_data=pd.DataFrame(cursor.fetchall())
    cursor.close()
    return read_data


# List of Tables
def table_list():
    cursor=connection.cursor()
    cursor.execute("SHOW TABLES FROM "+db_name)
    tables=[i[0] for i in cursor.fetchall()]
    cursor.close()
    return tables