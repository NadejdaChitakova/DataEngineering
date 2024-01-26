import pyodbc
import pandas as pd
import load_data_to_db as loadData

def get_data_from_table(connection):
    cursor = connection.cursor()
    query = f" SELECT * FROM movies"
    
    cursor.execute(query)

    df = pd.DataFrame.from_records(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    cursor.close()
    connection.close()
    return df

def load_to_table(connection,databaseName,df):
    cursor = connection.cursor()
    for index, row in df.iterrows():
        values = tuple(row)
        query = f'INSERT INTO movies VALUES {values}'
        print(query)
        cursor.execute(query)
    connection.commit()
    cursor.close()
    connection.close()

