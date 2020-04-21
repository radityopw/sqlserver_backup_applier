## create config.py
`
import sqlalchemy
import ftplib

def ftp_source():
    return ftplib.FTP('<HOST>','<username>','<PASSWORD>')

def monitoring_db():
    user = "root"
    password = ""
    database = "sqlserver_backup_applier"
    port = "3306"
    server = "127.0.0.1"
    engine = "mysql+pymysql"
    args = ""

    return sqlalchemy.create_engine(engine+'://'+user+':'+password+'@'+server+':'+port+'/'+database+'?'+args)

def sqlserver_source():
    user = ""
    password = ""
    database = ""
    port = "1433"
    server = ""
    engine = "mssql+pyodbc"
    args = "driver=ODBC Driver 17 for SQL Server"

    return sqlalchemy.create_engine(engine+'://'+user+':'+password+'@'+server+':'+port+'/'+database+'?'+args)

def sqlserver_target():
    user = ""
    password = ""
    database = ""
    port = "1433"
    server = ""
    engine = "mssql+pyodbc"
    args = "driver=ODBC Driver 17 for SQL Server"

    return sqlalchemy.create_engine(engine+'://'+user+':'+password+'@'+server+':'+port+'/'+database+'?'+args)
`