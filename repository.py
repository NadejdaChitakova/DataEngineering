import pyodbc

username = 'sa'
password = 'Na!12345678'

def InitDatabase(server):
    connString = f'DRIVER={{SQL Server}};SERVER={server};DATABASE=master;UID={username};PWD={password};TrustServerCertificate=YES'
    conn = InitConnection(connString)
    cursor = conn.cursor()
    CreateDatabase(cursor)
    cursor.close()
    conn.close()

def ConnectToDB(server):
    connectionString = f'DRIVER={{SQL Server}};SERVER={server};DATABASE=movies;UID={username};PWD={password};TrustServerCertificate=YES'
    connection = InitConnection(connectionString)
    return connection.cursor()

def GetConnection(server):
    connectionString = f'DRIVER={{SQL Server}};SERVER={server};DATABASE=movies;UID={username};PWD={password};TrustServerCertificate=YES'
    return InitConnection(connectionString)

def InitConnection(connectionString):
    connection = pyodbc.connect(connectionString,  autocommit=True)
    return connection

def CreateDatabase(cursor):
    cursor.execute('''IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='movies') CREATE DATABASE movies''')

def CreateCursor(port):
    InitDatabase(port)
    cursor = ConnectToDB(port)
    CreateDatabase(cursor)
    return cursor