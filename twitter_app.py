import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import socket
import json

consumer_key = 'LovGCp4taXgOGclj9Wum2XEda'
consumer_secret = 'd2NhvrqiEhsp8iBBZ1zsN5fl79AOwr0DOktoNqzIC56bK5NCLG'
access_token = '1323237425498435584-9TGePmcG2nq56MmN71K5jh7TBWd1AX'
access_secret = 'nhLAqoCicm4FJXwRw75MeQTeXxRu9L0Us4RqVxXgQP9AQ'


class TweetsListener(StreamListener):
    # initialized the constructor
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            # read the Twitter data which comes as a JSON format
            msg = json.loads(data)

            # the 'text' in the JSON file contains the actual tweet.
            print(msg['text'].encode('utf-8'))

            # the actual tweet data is sent to the client socket
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True

        except BaseException as e:
            # Error handling
            print("Ahh! Look what is wrong : %s" % str(e))
            return True

    def on_error(self, status):
        print(status)
        return True


def send_data(c_socket):
    # authentication
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    # twitter_stream will get the actual live tweet data
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    # filter the tweet feeds related to "corona"
    twitter_stream.filter(track=['corona'])
    # in case you want to pass multiple criteria
    # twitter_stream.filter(track=['DataScience','python','Iot'])


if __name__ == "__main__":
    # create a socket object
    s = socket.socket()

    # Get local machine name : host and port
    host = "127.0.0.1"
    port = 3333

    # Bind to the port
    s.bind((host, port))
    print("Listening on port: %s" % str(port))

    # Wait and Establish the connection with client.
    s.listen(5)
    c, addr = s.accept()

    print("Received request from: " + str(addr))

    # Keep the stream data available
    send_data(c)
