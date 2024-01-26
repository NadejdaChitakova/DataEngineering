from ETL_Process.db_connector import get_data_from_table, load_to_table
import pandas as pd
import load_data_to_db as loadData
import repository as repo

destinationTablePort = 'localhost,14333'
movieTablePort = 'localhost,1433'
topMoviePort = 'localhost,14331'
netflixPort = 'localhost,14332'

def execute_load():
    destinationCursor = repo.CreateCursor(destinationTablePort)
    movieTableConnection = repo.GetConnection(movieTablePort)
    topMovieConnection = repo.GetConnection(topMoviePort)
    netflixConnection = repo.GetConnection(netflixPort)

    #### Create Dest Database table
    destinationCursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='movies' and xtype='U')
            create table movies (
            i int,
            id varchar(255),
            title varchar(8000),
            description varchar(8000),
            release_year varchar(20),
            genre varchar(255)
        )""")

    ## load movie -> destination table
    dfMovie = get_data_from_table(movieTableConnection)
    dfMovie = dfMovie.rename(columns = {'orig_title': 'title', 'overview': 'description', 'date_x': 'release_year'})
    dfMovie = dfMovie[[ 'title', 'description', 'release_year', 'genre']]

    load_data_to_destionaton_table(dfMovie, destinationCursor)

    topMoviesDF =  get_data_from_table(topMovieConnection)
    topMoviesDF = topMoviesDF.rename(columns = {'Series_Title': 'title', 'Overview': 'description', 'Released_Year': 'release_year', 'Genre': 'genre'})
    topMoviesDF = topMoviesDF[[ 'title', 'description', 'release_year', 'genre']]

    load_data_to_destionaton_table(topMoviesDF, destinationCursor)

    netflixDF =  get_data_from_table(netflixConnection)
    netflixDF = netflixDF.rename(columns = {'title': 'title', 'description': 'description', 'released_year': 'release_year', 'type': 'genre'})
    netflixDF = netflixDF[[ 'title', 'description', 'release_year', 'genre']]

    load_data_to_destionaton_table(topMoviesDF, destinationCursor)


def load_data_to_destionaton_table(dataFrame, cursor):
    for i,row in dataFrame.iterrows():
        cursor.execute('''
                    INSERT INTO movies (i,title,description,release_year,genre)
                    VALUES (?,?,?,?,?)''',
                    i, 
                    row.title,
                    row.description,
                    row.release_year,
                    row.genre,
                    )
