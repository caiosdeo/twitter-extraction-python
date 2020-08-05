import configparser
import sqlite3
from sqlite3 import Error

config = configparser.ConfigParser()
config.read('config.ini')

def createDatabaseConnection():
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
    :param profile: (username, already_done)
    """
    sql = ''' INSERT INTO Profiles(username,already_done) VALUES (?,?) '''
    cur = conn.cursor()
    cur.execute(sql, profile)
    conn.commit()

def updateProfile(conn, profile):
    """
    update already_done of a profile by its id
    :param conn:
    :param profile: (already_done, id)
    """
    sql = ''' UPDATE Profiles SET already_done = ? WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, profile)
    conn.commit()

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
    Query for the first row in the profiles table with already_done equals to 0
    :param conn: the Connection object
    :return: notDoneProfile tuple 0 - id; 1 - username; 3 - already_done
    """

    cur = conn.cursor()
    cur.execute("SELECT * FROM Profiles WHERE already_done = 0 ORDER BY id ASC LIMIT 1 ")

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
                                    already_done integer NOT NULL
                                );"""

    conn = createDatabaseConnection()

    with conn:
        # * create profiles table
        createTable(conn, sql_create_profiles_table)

        # * inserting data into profiles
        with open(config['FILES']['INITIAL_PROFILE_SET'], "r") as initialSet:
            profiles = initialSet.read().splitlines()
            for profile in profiles:
                # Inserting the username and 0 for not concluded 
                createProfile(conn, (profile, 0) )


if __name__ == "__main__":
    try:
        mainDB()    
    except Exception as ex:
        print("Exception on main occured: " + str(ex))