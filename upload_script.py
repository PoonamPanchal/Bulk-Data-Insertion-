import pandas as pd
import numpy as np
import pymysql 
import glob
import os
import time
path = os.getcwd()
csv_files = glob.glob(os.path.join(path, "*.csv"))
for f in csv_files:
    
    f=os.path.splitext(f)[0].split("\\")[-1]
    print("your file name is : ", f)
    db=input("Enter Database name: ")

    df=pd.read_csv(f+'.csv',index_col=False)
    time.sleep(5)
    df1 = df.replace(np.nan, '', regex=True)
    tpls = list(df1.itertuples(index=False, name=None))

    columns=pd.read_csv(f+".csv",nrows=0).columns.tolist()
    conn_params_dic = {
        "host"      : "localhost",
        "user"      : "root",
        "password"  : ""
    }
    #connect to mysql 
    conn = pymysql.connect(**conn_params_dic)
    time.sleep(5)
    cursor = conn.cursor()

    #Create Database
    cursor.execute("CREATE DATABASE IF NOT EXISTS %s" % db )
    conn_params_dic["database"]=db
    #make connection with database
    conn = pymysql.connect(**conn_params_dic)
    time.sleep(5)
    cursor = conn.cursor()

    #columns auto creation
    s=[]
    for i in columns:
        s.append("`"+i.strip()+"` VARCHAR(255)")
    cols=', '.join(map(str,s))
    
    modulos=[]+len(s)*['%s']
    modulos=', '.join(map(str,modulos))

    #Create table
    Create_query="""CREATE TABLE IF NOT EXISTS `"""+ f +"` ("+ cols + ")"
    cursor.execute(Create_query)
    time.sleep(5)
    #Insert into Database 
    try:
        insert_query="INSERT INTO `" + f +"` VALUES("+ modulos +")"
        time.sleep(5)

        cursor.executemany(insert_query,tpls)
        conn.commit()
        print("REcord inserttion successful in database:", db)

    except:
        print("Failed to insert records in db ")
