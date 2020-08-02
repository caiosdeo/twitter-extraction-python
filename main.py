from twython import Twython
from datetime import datetime
import json
import time
import urllib.request
import sys
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

def getConnection(key):
    """
    @param key Varies from 1 to 4 to choose the credentials
    @return Instancia do twitter
    """

    # Dicionário para definir o conjunto das credenciais
    creds = {
        '1': config['TWITTER1'],
        '2': config['TWITTER2'],
        '3': config['TWITTER3'],
        '4': config['TWITTER4'],
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
        with open("output/log/log" + str(key) + ".log", "w") as log:
            log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] Iniciando profile: " + username + "\n")
            while True:
                try:
                    ids = twitter.get_followers_list(screen_name=username, cursor=cursor)
                except Exception as ex:
                    log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] Failed to get followers' ids: " + str(ex) + "\n");
                    fw.flush()
                    log.flush()
                    time.sleep(900000);
                    while not netIsAvailable():
                        log.write("[" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "] No internet. Waiting 5 minutes to try again..\n");
                        time.sleep(300000);
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

def main(key):

    twitter = getConnection(key)

    # Abrindo arquivo para ver os perfis com seguidores já extraídos
    with open(config['FILES']['DONE_SET'], 'r+') as doneSet:
        
        doneProfiles = doneSet.read().splitlines()
        if len(doneProfiles) > 0:
            lastProfileDone = doneProfiles[len(doneProfiles) - 1]
        else: 
            lastProfileDone = ""

        lastProfileFound = False

        with open(config['FILES']['INITIAL_PROFILE_SET'], "r") as initialSet:
            profiles = initialSet.read().splitlines()
            for profile in profiles:

                if lastProfileFound or lastProfileDone == "":
                    getFollowers(twitter, profile, -1, key)
                    # Salvando o último username a ter todos seguidores extraídos
                    doneSet.write(profile + "\n")

                elif profile == lastProfileDone:
                    lastProfileFound = True

            print("Every follower from the seeds extracted")


if __name__ == "__main__":
    try:
        main(sys.argv[1])    
    except Exception as ex:
        print("Exception on main occured: " + str(ex))