# -*- coding: utf-8 -*-
from twython import Twython
from datetime import datetime
import json
import time
import urllib.request
import sys
import database as db

def exportFollowers(conn, idProfile, username):

    print("Exporting followers -> " + username)

    with open("output/followers/" + username + ".csv", "w") as fw:

        try:
            followers = db.selectFollowersByProfileId(conn, idProfile)

            for follower in followers:

                fw.write(
                    str(follower[1]) + "," + 
                    str(follower[2]) + "," + 
                    str(follower[3]) + "," + 
                    str(follower[4]) + "," + 
                    str(follower[5]) + "," + 
                    str(follower[6]) + "," + 
                    str(follower[7]) + "," + 
                    str(follower[8]) + "," + 
                    str(follower[9]) + "," + 
                    str(follower[10]) + "," + 
                    str(follower[11]) + "," + 
                    str(follower[12]) + "," + 
                    str(follower[13]) + "\n"),

        except Exception as ex:
            print("Exception on writing followers ( " + username + " ): " + str(ex))      

    print("Exported followers -> " + username)

def main():

    # Connects to SQLite DB
    conn = db.createDatabaseConnection("resources/database-fixed.db")

    numberOfProfiles = db.countProfiles(conn)

    # First follower inserted at the database
    i = db.selectFollowerById(conn, 1)[16]
    
    profile = db.selectProfileById(conn, i)

    while i <= numberOfProfiles:

        print("idProfile: " + str(i) + " of " + str(numberOfProfiles))

        # Username to name the csv
        username = profile[1]

        exportFollowers(conn, i, username)

        i += 1

        try:
            profile = db.selectProfileById(conn, i)
        except Exception as ex:
            print("Exception trying to get profile by id: " + str(ex))


if __name__ == "__main__":
    try:
        main()    
    except Exception as ex:
        print("Exception on main occured: " + str(ex))
