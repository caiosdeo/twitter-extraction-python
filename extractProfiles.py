# -*- coding: utf-8 -*-
from twython import Twython
from datetime import datetime
import json
import time
import urllib.request
import sys
import configparser
import database as db

config = configparser.ConfigParser()
config.read('config.ini')

def getConnection(key):
    """
    :param: key Varies from 1 to 4 to choose the credentials
    :return: Instancia do twitter
    """

    # Dicionário para definir o conjunto das credenciais
    creds = {
        1: config['TWITTER1'],
        2: config['TWITTER2'],
        3: config['TWITTER3'],
        4: config['TWITTER4'],
    }.get(key, "Invalid set")

    # Instanciando um objeto
    return Twython(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'])

def netIsAvailable():

    try:
        urllib.request.urlopen('http://google.com')
        return True
    except:
        return False


def getFollowers(twitter, conn, idProfile, username, cursor, key):
    print("Listing followers' ids -> " + username)

    with open("output/followers/" + username + ".csv", "w") as fw:
        with open("output/log/log" + str(key) + ".log", "a") as log:
            with open("resources/nextSet" + str(key) + ".txt", "a") as nextSet:
                log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] Iniciando profile: " + username + "\n")
                while True:
                    try:
                        ids = twitter.get_followers_list(screen_name=username, cursor=cursor, count=200)                    
                    except Exception as ex:
                        log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] Failed to get followers' ids: " + str(ex) + "\n");
                        fw.flush()
                        log.flush()
                        if(ex.error_code == 429 or ex.error_code == 500):
                            log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] Rate limit exceeded. Waiting 15 minutes to continue..\n");
                            time.sleep(900);
                            while not netIsAvailable():
                                log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] No internet. Waiting 5 minutes to try again..\n");
                                time.sleep(300);
                            twitter = getConnection(key);
                            try:
                                ids = twitter.get_followers_list(screen_name=username, cursor=cursor, count=200)
                            except Exception as ex:
                                if(ex.error_code == 401 or ex.error_code == 404):
                                    break
                        else:
                            log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] Private or invalid profile. Skipping..\n");
                            break;
                    log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] Writing " + str(len(ids['users'])) + " data...\n");

                    for id in ids['users']:

                        if not (id["protected"]):

                            geo = 1 if id['geo_enabled'] else 0

                            db.insertFollower(conn, (str(id['screen_name']), str(id['id']), str(id['followers_count']), str(id['friends_count']), id['location'], geo, str(id['lang']), str(id['statuses_count']), '', str(id['created_at']), str(id['verified']), str(id['default_profile']), str(id['default_profile_image']), 0, 0, idProfile))
                            fw.write(str(id['screen_name']) + ";" + str(id['id']) + ";" + str(id['followers_count']) + ";" + str(id['friends_count']) + ";" + id['location'] + ";" + str(id['lang']) + ";" + str(id['statuses_count']) + str(id['created_at']) + ";" + str(id['verified']) + ";" + str(id['default_profile']) + ";" + str(id['default_profile_image']) + "\n");
                            nextSet.write(str(id['screen_name']) + "\n")

                    conn.commit()
                    cursor = ids['next_cursor']
                    if (cursor == 0):
                        break   

    print("Listed followers' ids -> " + username)

def main(key):

    # Auth to twitter API
    twitter = getConnection(key)

    # Connects to SQLite DB
    conn = db.createDatabaseConnection()

    profile = db.selectFirstNotDoneProfile(conn)
    while profile != None:

        # Salva no banco que o perfil está sendo usado
        db.updateProfileBeingUsed(conn, (1, profile[0]))

        getFollowers(twitter, conn, profile[0], profile[1], -1, key)
        
        # Salvando o último username a ter todos seguidores extraídos
        db.updateProfileDone(conn, (1, 0, profile[0]))

        profile = db.selectFirstNotDoneProfile(conn)


if __name__ == "__main__":
    try:
        time.sleep(15)
        main(2)    
    except Exception as ex:
        print("Exception on main(2) occured: " + str(ex))
