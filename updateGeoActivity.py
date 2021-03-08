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

def timelineRequestIsAvailable(twitter, username):

    try:
        tweetList = twitter.get_user_timeline(screen_name=username)
        return True
    except Exception as ex:
        if(ex.error_code == 429):
            return False

def updateGeoActivity(twitter, conn, profile, cursor, key):
    print("Gathering followers' geo and activity -> " + profile[1])

    with open("output/log/log" + str(key) + ".log", "a") as log:
        log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] Updating profile geo and activity: " + profile[1] + "\n")
        while True:

            try:
                tweetList = twitter.get_user_timeline(screen_name=profile[1])
            except Exception as ex:
                log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] Error: " + str(ex) + "..Next profile... \n");
                log.flush()
                if(ex.error_code == 429 or ex.error_code == 500):

                    while not timelineRequestIsAvailable(twitter, profile[1]):
                        log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] Rate limit exceeded. Waiting 15 minutes to try again..\n");
                        time.sleep(900);
                
                    while not netIsAvailable():
                        log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] No internet. Waiting 5 minutes to try again..\n");
                        time.sleep(300);

                    twitter = getConnection(key);
                    tweetList = twitter.get_user_timeline(screen_name=profile[1])
                else:
                    break

            if (len(tweetList) > 1):
                tweetTime = tweetList[0]['created_at'] + ";" + tweetList[len(tweetList) - 1]['created_at'] + ";" + str(len(tweetList));

            elif (len(tweetList)  == 1):
                tweetTime = tweetList[0]['created_at'] + ";;1"
            
            elif (len(tweetList)  == 0):
                tweetTime = ";;0"

            # Se o usuário tem o geo_enabled ativo, algum tweet dele pode vir com coordenadas exatas
            if(profile[6] == 1): 
                for tweet in tweetList:
                    if(tweet['geo']):
                        location = tweet['geo']['coordinates']
                        db.updateFollowerLocation(conn, (str(location), profile[0]))
                        break   

            db.updateFollowerTweetTime(conn, (tweetTime, profile[0]))
            break
    print("Gathered followers' geo and activity -> " + profile[1])

def main(key):

    # Auth to twitter API
    twitter = getConnection(key)

    # Connects to SQLite DB
    conn = db.createDatabaseConnection()

    profile = db.selectFirstNotDoneFollower(conn)

    while profile != None:

        # Salva no banco que o perfil está sendo usado
        db.updateFollowerBeingUsed(conn, (1, profile[0]))

        updateGeoActivity(twitter, conn, profile, -1, key)

        # Salvando o último username a ter todos seguidores extraídos
        db.updateFollowerDone(conn, (1, 0, profile[0]))

        try:
            profile = db.selectFirstNotDoneFollower(conn)
        except:
            time.sleep(300)
            profile = db.selectFirstNotDoneFollower(conn)


if __name__ == "__main__":
    try:
        # Waiting other services to fill up some data on followers table on database
        time.sleep(60)
        main(4)    
    except Exception as ex:
        print("Exception on main(4) occured: " + str(ex))
