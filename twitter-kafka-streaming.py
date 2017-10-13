# ________         _______________            ____________                               
# ___  __/__      ____(_)_  /__  /______________  ___/_  /__________________ _______ ___ 
# __  /  __ | /| / /_  /_  __/  __/  _ \_  ___/____ \_  __/_  ___/  _ \  __ `/_  __ `__ \
# _  /   __ |/ |/ /_  / / /_ / /_ /  __/  /   ____/ // /_ _  /   /  __/ /_/ /_  / / / / /
# /_/    ____/|__/ /_/  \__/ \__/ \___//_/    /____/ \__/ /_/    \___/\__,_/ /_/ /_/ /_/ 
#                                                                                       

import tweepy
import threading, logging, time
from kafka.client import KafkaClient
# from kafka.consumer import SimpleConsumer
# from kafka.producer import SimpleProducer
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
import string
import os
import atexit
import logging

######################################################################
# Authentication details. To  obtain these visit dev.twitter.com
######################################################################

CONSUMER_KEY = os.environ['CONSUMER_KEY']
CONSUMER_SECRET = os.environ['CONSUMER_SECRET']
ACCESS_TOKEN = os.environ['ACCESS_TOKEN']
ACCESS_TOKEN_SECRET = os.environ['ACCESS_TOKEN_SECRET']

topic_name='twitter-kafka'# ex. 'twitterstream', or 'test' ...
kafka_broker = "localhost:9092"

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

######################################################################
#Create a handler for the streaming data that stays open...
######################################################################

class MyStreamListener(tweepy.StreamListener):

    #Handler
    ''' Handles data received from the stream. '''

    ######################################################################
    #For each status event
    ######################################################################

    def on_status(self, status):

        producer = KafkaProducer(bootstrap_servers=kafka_broker)
        
        # Prints the text of the tweet
        #print '%d,%d,%d,%s,%s' % (status.user.followers_count, status.user.friends_count,status.user.statuses_count, status.user.id_str, status.user.screen_name)
        
        # Schema changed to add the tweet text
        #print '%d,%d,%d,%s,%s' % (status.user.followers_count, status.user.friends_count,status.user.statuses_count, status.text, status.user.screen_name)
        #message =  str(status.user.followers_count) + ',' + str(status.user.friends_count) + ',' + str(status.user.statuses_count) + ',' + status.text + ',' + status.user.screen_name
        message = str(status.text)
        msg = filter(lambda x: x in string.printable, message)
        msg = str(msg)
        payload = ('{"TweetStatus":"%s","DateTime":"%s"}' % (msg, time.time())).encode('utf-8')


        logger.debug('Retrieved tweeter status %s', payload)
        logger.debug('Sent tweeter status for %s to Kafka', message)

        try:
            producer.send(topic=topic_name, value=payload, timestamp_ms=time.time())
            #print(msg,"printing message")
        except Exception as e:
            raise e
        
        return True
       
    ######################################################################
    #Supress Failure to keep demo running... In a production situation 
    #Handle with seperate handler
    ######################################################################
 
    def on_error(self, status_code):

        print('Got an error with status code: ' + str(status_code))
        return True # To continue listening
 
    def on_timeout(self):

        print('Timeout...')
        return True # To continue listening


    def shutdown_hook(producer):
        """
        a shutdown hook to be called before the shutdown
        :param producer: instance of a kafka producer
        :return: None
        """
        try:
            logger.info('Flushing pending messages to kafka, timeout is set to 10s')
            producer.flush(10)
            logger.info('Finish flushing pending messages to kafka')
        except KafkaError as kafka_error:
            logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
        finally:
            try:
                logger.info('Closing kafka connection')
                producer.close(10)
            except Exception as e:
                logger.warn('Failed to close kafka connection, caused by: %s', e.message)

######################################################################
#Main Loop Init
######################################################################


if __name__ == '__main__':
    
    #listener = StdOutListener()
    listener = MyStreamListener()

    #sign oath cert

    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)

    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    #uncomment to use api in stream for data send/retrieve algorythms 
    #api = tweepy.API(auth)
    #api = tweepy.API(auth)

    stream = tweepy.Stream(auth=auth, listener=listener)

    ######################################################################
    #Sample delivers a stream of 1% (random selection) of all tweets
    ######################################################################
    #stream.sample()
    stream.filter(track=['$btc'])
    # client = KafkaClient("localhost:9092")
    # producer = SimpleProducer(client)

 

    atexit.register(shutdown_hook, producer)

    ######################################################################
    #Custom Filter rules pull all traffic for those filters in real time.
    #Bellow are some examples add or remove as needed...
    ######################################################################
    #A Good demo stream of reasonable amount
    #stream.filter(track=['actian', 'BigData', 'Hadoop', 'Predictive', 'Quantum', 'bigdata', 'Analytics', 'IoT'])
    #Hadoop Summit following
#stream.filter(track=['actian', 'hadoop', 'hadoopsummit'])