import pandas as pd
import repository as repo

def LoadDataToTables():
    imdbMovies = pd.read_csv (r'./resources/imdb_movies.csv')
    imdbTopMovies = pd.read_csv (r'./resources/imdb_top_1000.csv')
    imdbNetflix = pd.read_csv (r'./resources/Netflix TV Shows and Movies.csv')

    moviesServer = 'localhost,1433'
    repo.InitDatabase(moviesServer)
    moviesDF = CreateDataFrame(imdbMovies)
    cursor = repo.ConnectToDB(moviesServer)
    repo.CreateDatabase(cursor)

    cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='movies' and xtype='U')
            create table movies (
            names varchar(200),
            date_x varchar(20),
            score decimal(5,1),
            genre varchar(200),
            overview varchar(1000),
            crew varchar(2000),
            orig_title varchar(500),
            status varchar (50),
            orig_lang varchar(300),
            budget_x decimal(20,2),
            revenue decimal(20,2),
            country varchar(2) 
        )""")

    header = GetTableHeaders(moviesDF)

    for row in moviesDF.itertuples():
            print(row)
            cursor.execute('''
                        INSERT INTO movies ({})
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'''.format(header),
                        row.names, 
                        row.date_x,
                        row.score,
                        row.genre,
                        row.overview,
                        row.crew,
                        row.orig_title,
                        row.status,
                        row.orig_lang,
                        row.budget_x,
                        row.revenue,
                        row.country)
    
    movieTopServer = 'localhost,14331'
    repo.InitDatabase(movieTopServer)
    topMovieDF = CreateDataFrame(imdbTopMovies)
    topMovieCursor = repo.ConnectToDB(movieTopServer)
    repo.CreateDatabase(topMovieCursor)

    headerTopMovies = GetTableHeaders(topMovieDF)

    topMovieCursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='movies' and xtype='U')
            create table movies (
            Poster_Link varchar(500),
            Series_Title varchar(255),
            Released_Year varchar(20),
            Certificate varchar(1000),
            Runtime varchar(1000),
            Genre varchar(1000),
            IMDB_Rating decimal(20,2),
            Overview varchar(500),
            Meta_score decimal(10,2),
            Director varchar(100),
            Star1 varchar(100),
            Star2 varchar(100),
            Star3 varchar(100),
            Star4 varchar(100),
            No_of_Votes decimal(20),
            Gross varchar(50)    )""")
    print(headerTopMovies)
    for row in topMovieDF.itertuples():
            print(row)
            topMovieCursor.execute('''
                        INSERT INTO movies ({})
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''.format(headerTopMovies),
                        row.Poster_Link, 
                        row.Series_Title,
                        row.Released_Year,
                        row.Certificate,
                        row.Runtime,
                        row.Genre,
                        row.IMDB_Rating,
                        row.Overview,
                        row.Meta_score,
                        row.Director,
                        row.Star1,
                        row.Star2,
                        row.Star3,
                        row.Star4,
                        row.No_of_Votes,
                        row.Gross)

    netflixServer = 'localhost,14332'
    repo.InitDatabase(netflixServer)
    netflixDF = CreateDataFrame(imdbNetflix)
    netflixCursor = repo.ConnectToDB(netflixServer)
    repo.CreateDatabase(netflixCursor)

    fullHeaderNetflix = GetTableHeaders(netflixDF)
    netflixHeader = fullHeaderNetflix.removeprefix('index, ')

    netflixCursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='movies' and xtype='U')
            create table movies (
            id varchar(255),
            title varchar(500),
            type varchar(100),
            description varchar(8000),
            release_year varchar(20),
            age_certification varchar(10),
            runtime decimal(10),
            imdb_id varchar(200),
            imdb_score decimal(10,2),
            imdb_votes decimal(20,2) 
        )""")


    for row in netflixDF.itertuples():
            netflixCursor.execute('''
                        INSERT INTO movies ({})
                        VALUES (?,?,?,?,?,?,?,?,?,?)'''.format(netflixHeader),
                        row.id,
                        row.title,
                        row.type,
                        row.description,
                        row.release_year,
                        row.age_certification,
                        row.runtime,
                        row.imdb_id,
                        row.imdb_score,
                        row.imdb_votes
                        )

def CreateDataFrame(file):
    dataFrame = pd.DataFrame(file)
    dataFrame.columns = file.columns.values.tolist()
    dataFrame.fillna(0, inplace=True)
    return dataFrame

def GetTableHeaders(dataframe):
    result = ''
    for x in dataframe.keys().values.tolist():
        result += ', '+ x
    return result.removeprefix(', ')

