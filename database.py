import configparser
from tqdm import tqdm
import sqlite3
from sqlite3 import Error

config = configparser.ConfigParser()
config.read('config.ini')

def connect():
    """ create a database connection to the SQLite database specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(config['SQLITE']['DATABASE'])
        return conn
    except Error as e:
        print(e)

    return conn

def createDatabaseConnection():
    """ create a database connection to the SQLite database specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(config['SQLITE']['DATABASE'], timeout=40, check_same_thread=False)
        conn.execute('pragma journal_mode=wal')
        return conn
    except Error as e:
        print(e)

    return conn  

def createTable(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def createProfile(conn, profile):
    """
    Create a new profile into the profiles table
    :param conn:
    :param profile: (username, already_done, in_extraction)
    """
    sql = ''' INSERT INTO Profiles(username,already_done,in_extraction) VALUES (?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, profile)
    # conn.commit()

def insertFollower(conn, follower):
    """
    Create a new follower into the followers table
    :param conn:
    :param follower: (username, twitter_id, followers_count, friends_count,
    location, geo, lang, statuses_count, tweet_time, created_at,
    verified, default_profile, default_profile_image, 
    already_done, in_extraction, idProfile)
    """
    sql = ''' INSERT INTO Follower(username, twitter_id, followers_count, friends_count, location, geo, lang, statuses_count, tweet_time, created_at, verified, default_profile, default_profile_image, already_done, in_extraction, idProfile) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, follower)
    # conn.commit()

def updateFollowerTweetTime(conn, follower):
    """
    update tweet_time of a follower by its id
    :param conn:
    :param follower: (tweet_time, id)
    """
    sql = ''' UPDATE Follower SET tweet_time = ? WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, follower)
    conn.commit()

def updateFollowerLocation(conn, follower):
    """
    update location of a follower by its id
    :param conn:
    :param follower: (location, id)
    """
    sql = ''' UPDATE Follower SET location = ? WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, follower)
    conn.commit()

def updateProfileBeingUsed(conn, profile):
    """
    update in_extraction of a profile by its id
    :param conn:
    :param profile: (in_extraction, id)
    """
    sql = ''' UPDATE Profiles SET in_extraction = ? WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, profile)
    conn.commit()

def updateFollowerBeingUsed(conn, follower):
    """
    update in_extraction of a follower by its id
    :param conn:
    :param follower: (in_extraction, id)
    """
    sql = ''' UPDATE Follower SET in_extraction = ? WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, follower)
    conn.commit()

def updateProfileDone(conn, profile):
    """
    update already_done of a profile by its id
    :param conn:
    :param profile: (already_done, in_extraction, id)
    """
    sql = ''' UPDATE Profiles SET already_done = ?, in_extraction = ? WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, profile)
    conn.commit()

def updateFollowerDone(conn, follower):
    """
    update already_done of a follower by its id
    :param conn:
    :param follower: (already_done, in_extraction, id)
    """
    sql = ''' UPDATE Follower SET already_done = ?, in_extraction = ? WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, follower)
    conn.commit()

def updateProfileNotDone(conn, profile):
    """
    update already_done of a profile by its id
    :param conn:
    :param profile: (already_done, in_extraction, id)
    """
    sql = ''' UPDATE Profiles SET already_done = ?, in_extraction = ? WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, profile)
    conn.commit()

def updateFollowerNotDone(conn, follower):
    """
    update already_done of a follower by its id
    :param conn:
    :param follower: (already_done, in_extraction, id)
    """
    sql = ''' UPDATE Follower SET already_done = ?, in_extraction = ? WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, follower)
    conn.commit()

def selectProfile(conn, username):
    """
    Query all rows in the profiles table searching for an specific username
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM Profiles WHERE username = ?", (username, ))

    username = cur.fetchall()

    if len(username) == 0:
        return None
    return username

def selectAllProfiles(conn):
    """
    Query all rows in the profiles table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM Profiles")

    rows = cur.fetchall()

    for row in rows:
        print(row)

def selectFirstNotDoneProfile(conn):
    """
    Query for the first row in the profiles table with already_done equals to 0 and
    if the profile is not being used by other set of keys
    :param conn: the Connection object
    :return: notDoneProfile tuple 0 - id; 1 - username; 3 - already_done; 4 - in_extraction
    """

    cur = conn.cursor()
    cur.execute("SELECT * FROM Profiles WHERE already_done = 0 AND in_extraction = 0 ORDER BY id ASC LIMIT 1 ")

    notDone = cur.fetchall()

    if len(notDone) == 0:
        return None

    return notDone[0]

def selectFirstNotDoneFollower(conn):
    """
    Query for the first row in the followers table with already_done equals to 0 and
    if the follower is not being used by other set of keys
    :param conn: the Connection object
    :return: notDoneFollower tuple 0 - id; 1 - username; 3 - already_done; 4 - in_extraction; 5 - idProfile
    """

    cur = conn.cursor()
    cur.execute("SELECT * FROM Follower WHERE already_done = 0 AND in_extraction = 0 ORDER BY id ASC LIMIT 1 ")

    notDone = cur.fetchall()

    if len(notDone) == 0:
        return None

    return notDone[0]

def countProfiles(conn):
    """
    Query to retrieve how many rows profiles table has
    :param conn: the Connection object
    :return: count int
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM Profiles;")

    count = cur.fetchall()

    return count[0][0]

def mainDB():

    sql_create_profiles_table = """CREATE TABLE IF NOT EXISTS Profiles (
                                    id integer PRIMARY KEY,
                                    username text NOT NULL,
                                    already_done integer NOT NULL,
                                    in_extraction integer NOT NULL
                                );"""

    sql_create_followers_table = """CREATE TABLE IF NOT EXISTS Follower (
                                        id integer PRIMARY KEY, 
                                        username text NOT NULL, 
                                        twitter_id text,
                                        followers_count text,
                                        friends_count text,
                                        location text, 
                                        geo int,
                                        lang text,
                                        statuses_count text,
                                        tweet_time text, 
                                        created_at text, 
                                        verified text,
                                        default_profile text,
                                        default_profile_image text,  
                                        already_done integer NOT NULL,
                                        in_extraction integer NOT NULL,
                                        idProfile integer,

                                        FOREIGN KEY (idProfile) REFERENCES Profiles(id)
                                    );"""

    conn = connect()

    with conn:

        # * create profiles table
        createTable(conn, sql_create_profiles_table)

        # * create followers table
        createTable(conn, sql_create_followers_table)

        # * inserting data into profiles
        with open(config['FILES']['LEVEL_ONE_SET'], "r") as dataset:
            profiles = dataset.read().splitlines()
            for profile in tqdm(profiles):
                # Verify if the profile is already on the database
                if selectProfile(conn, profile) == None:
                    # Inserting the username and 0 for not concluded and not being used
                    createProfile(conn, (profile, 0, 0) )

            conn.commit()
            print("Perfis inseridos no banco de dados")

if __name__ == "__main__":
    try:
        mainDB()    
    except Exception as ex:
        print("Exception on main occured: " + str(ex))