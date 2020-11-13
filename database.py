import configparser
from tqdm import tqdm
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
    :param profile: (username, already_done, in_extraction)
    """
    sql = ''' INSERT INTO Profiles(username,already_done,in_extraction) VALUES (?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, profile)
    # conn.commit()

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

    conn = createDatabaseConnection()

    with conn:
        # * create profiles table
        createTable(conn, sql_create_profiles_table)

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