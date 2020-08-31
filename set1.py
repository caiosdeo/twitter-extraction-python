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
    

def getFollowers(twitter, username, cursor, key):
    print("Listing followers's ids -> " + username)

    with open("output/followers/" + username + ".csv", "w") as fw:
        with open("output/log/log" + str(key) + ".log", "a") as log:
            log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] Iniciando profile: " + username + "\n")
            while True:
                try:
                    ids = twitter.get_followers_list(screen_name=username, cursor=cursor)                    
                except Exception as ex:
                    log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] Failed to get followers' ids: " + str(ex) + "\n");
                    fw.flush()
                    log.flush()
                    time.sleep(900);
                    while not netIsAvailable():
                        log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] No internet. Waiting 5 minutes to try again..\n");
                        time.sleep(300);
                    twitter = getConnection(key);
                    ids = twitter.get_followers_list(screen_name=username, cursor=cursor)
                
                log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] Writing " + str(len(ids['users'])) + " data...\n");

                for id in ids['users']:
                    tweetTime = ";;"
                    if not (id["protected"]):
                        try:
                            tweetList = twitter.get_user_timeline(screen_name=id['screen_name'])
                        except Exception as ex:
                            log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] Error: " + str(ex) + " on profile: " + id.getScreenName() + "..Next profile... \n");
                            continue

                        if (len(tweetList) > 1):
                            tweetTime = tweetList[0]['created_at'] + ";" + tweetList[len(tweetList) - 1]['created_at'] + ";" + str(len(tweetList));

                        elif (len(tweetList)  == 1):
                            tweetTime = tweetList[0]['created_at'] + ";;1"
                        
                        elif (len(tweetList)  == 0):
                            tweetTime = ";;0"
                    
                    fw.write(str(id['screen_name']) + ";" + str(id['id']) + ";" + str(id['followers_count']) + ";" + str(id['friends_count']) + ";" + str(id['location']) + ";" + str(id['lang']) + ";" + str(id['statuses_count']) + ";" + tweetTime + ";" + str(id['created_at']) + ";" + str(id['verified']) + ";" + str(id['default_profile']) + ";" + str(id['default_profile_image']) + "\n");
                cursor = ids['next_cursor']
                if (cursor == 0):
                    break   

    print("Listed followers's ids -> " + username)
      

def main(key):

    # Auth to twitter API
    twitter = getConnection(key)

    # Connects to SQLite DB
    conn = db.createDatabaseConnection()

    profile = db.selectFirstNotDoneProfile(conn)
    while profile != None:

        # Salva no banco que o perfil está sendo usado
        db.updateProfileBeingUsed(conn, (1, profile[0]))

        getFollowers(twitter, profile[1], -1, key)

        # Salvando o último username a ter todos seguidores extraídos
        db.updateProfileDone(conn, (1, 0, profile[0]))

        profile = db.selectFirstNotDoneProfile(conn)


    print("Every follower from the seeds extracted")


if __name__ == "__main__":
    try:
        main(1)    
    except Exception as ex:
        print("Exception on main occured: " + str(ex))