# -*- coding: utf-8 -*-
"""
Created on Fri Jul 22 19:04:47 2016

@author: Shreyans Shrimal
"""
import time
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
from textblob import TextBlob
import matplotlib.pyplot as plt
import re
import os

def calctime(a):
    return time.time()-a

positive=0
negative=0
compound=0

count=0
initime=time.time()
plt.ion()

 
 
consumer_key=os.environ['CONSUMER_KEY']
consumer_secret=os.environ['CONSUMER_SECRET']
access_token=os.environ['ACCESS_TOKEN']
access_token_secret=os.environ['ACCESS_TOKEN_SECRET']

class listener(StreamListener):
    
    def on_status(self,status):
        global initime
        t=int(calctime(initime))
        # all_data=json.loads(status)
        # tweet=all_data["text"].encode("utf-8")
        #username=all_data["user"]["screen_name"]

        tweet = str(status.text)
        tweet=" ".join(re.findall("[a-zA-Z]+", tweet))
        blob=TextBlob(tweet.strip())

        global positive
        global negative     
        global compound  
        global count
        
        count=count+1
        senti=0
        for sen in blob.sentences:
            senti=senti+sen.sentiment.polarity
            if sen.sentiment.polarity >= 0:
                positive=positive+sen.sentiment.polarity   
            else:
                negative=negative+sen.sentiment.polarity  
        compound=compound+senti        
        print(count)
        print(tweet.strip())
        print(senti)
        print(t)
        print(str(positive) + ' ' + str(negative) + ' ' + str(compound))
        
    
        plt.axis([ 0, 70, -20,20])
        plt.xlabel('Time')
        plt.ylabel('Sentiment')
        plt.plot([t],[positive],'go',[t] ,[negative],'ro',[t],[compound],'bo')
        plt.show()
        plt.pause(0.0001)
        if count==200:
            return False
        else:
            return True
        
    def on_error(self,status):
        print(status)


auth=OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_token_secret)

twitterStream=  Stream(auth, listener(count))
twitterStream.filter(track=["Donald Trump"])
      
 

