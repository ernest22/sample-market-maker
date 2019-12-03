import hashlib
import hmac
import json
import time
import urllib
from requests import Request, Session, Response
import requests

from websocket import create_connection

###
# websocket-apikey-auth-test.py
#
# Reference Python implementation for authorizing with websocket.
# See https://www.bitmex.com/app/wsAPI for more details, including a list
# of methods.
###

# These are not real keys - replace them with your keys.
API_KEY = "7ac720fe9e166525b410ed4549a8c03c"
API_SECRET = "0c8ed26f1a2b8a928b4f388bd997223b"

# Switch these comments to use testnet instead.
BITMEX_URL = "wss://api.bitkub.com"


VERB = "GET"
ENDPOINT = "/websocket-api/"


class BitkubClient:
    API_HOST = 'https://api.bitkub.com'

    def __init__(self, acc) -> None:
        self._session = Session()
        
        #change the account name
        account = acc
        self._account = account

        with open('api.json') as json_file:
            data = json.load(json_file)
            for p in data:
                exchange = p["exchange"]
                if (exchange == "Bitkub"):
                    name = p["name"]
                    if (name == account):
                        self._api_key = p["accesskey"]
                        secret = p["secretkey"]
                        self._api_secret = secret.encode()

    def json_encode(self, data):
        return json.dumps(data, separators=(',', ':'), sort_keys=True)

    def sign(self, data):
        j = self.json_encode(data)
        #print('Signing payload: ' + j)
        h = hmac.new(self._api_secret, msg=j.encode(), digestmod=hashlib.sha256)
        return h.hexdigest()

    # check server time
    def check_time(self):
        response = requests.get(self.API_HOST + '/api/servertime')
        ts = int(response.text)
        return(ts)
    
    def get_wstoken(self):
        header = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-BTK-APIKEY': self._api_key,
        }
        data = {
            'ts': self.check_time()
        }

        signature = self.sign(data)
        data['sig'] = signature
        response = requests.post(
            self.API_HOST + '/api/market/wstoken', headers=header, data=self.json_encode(data))
        token = json.loads(response.text)["result"]
        return(token)


def main():
    """Authenticate with the BitMEX API & request account information."""
    test_with_message()
    #test_with_querystring()


def test_with_message():
    # This is up to you, most use microtime but you may have your own scheme so long as it's increasing
    # and doesn't repeat.
    expires = int(time.time()) + 5
    # See signature generation reference at https://www.bitmex.com/app/apiKeys

    bkclient = BitkubClient("Gorn")
    wstoken = bkclient.get_wstoken()
    streamname = "market.trade.thb_btc"
    # Pinging Server
    #endpoint = ENDPOINT
    #ws = create_connection(BITMEX_URL + ENDPOINT + streamname)
    streamname = "1"
    ws = create_connection(BITMEX_URL + ENDPOINT + streamname)
    request = {"auth": wstoken}
    result = ws.recv()
    print("Received '%s'" % result)

    while(True):
        result = ws.recv()
        print("Received '%s'" % result)



    ws.close()


def test_with_querystring():
    # This is up to you, most use microtime but you may have your own scheme so long as it's increasing
    # and doesn't repeat.
    expires = int(time.time()) + 5
    # See signature generation reference at https://www.bitmex.com/app/apiKeys
    signature = bitmex_signature(API_SECRET, VERB, ENDPOINT, expires)

    # Initial connection - BitMEX sends a welcome message.
    ws = create_connection(BITMEX_URL + ENDPOINT +
                           "?api-expires=%s&api-signature=%s&api-key=%s" % (expires, signature, API_KEY))
    print("Receiving Welcome Message...")
    result = ws.recv()
    print("Received '%s'" % result)

    # Send a request that requires authorization.
    request = {"op": "subscribe", "args": "position"}
    ws.send(json.dumps(request))
    print("Sent subscribe")
    result = ws.recv()
    print("Received '%s'" % result)
    result = ws.recv()
    print("Received '%s'" % result)

    ws.close()


# Generates an API signature.
# A signature is HMAC_SHA256(secret, verb + path + nonce + data), hex encoded.
# Verb must be uppercased, url is relative, nonce must be an increasing 64-bit integer
# and the data, if present, must be JSON without whitespace between keys.
def bitmex_signature(apiSecret, verb, url, nonce, postdict=None):
    """Given an API Secret key and data, create a BitMEX-compatible signature."""
    data = ''
    if postdict:
        # separators remove spaces from json
        # BitMEX expects signatures from JSON built without spaces
        data = json.dumps(postdict, separators=(',', ':'))
    parsedURL = urllib.parse.urlparse(url)
    path = parsedURL.path
    if parsedURL.query:
        path = path + '?' + parsedURL.query
    # print("Computing HMAC: %s" % verb + path + str(nonce) + data)
    message = (verb + path + str(nonce) + data).encode('utf-8')
    print("Signing: %s" % str(message))

    signature = hmac.new(apiSecret.encode('utf-8'), message, digestmod=hashlib.sha256).hexdigest()
    print("Signature: %s" % signature)
    return signature

if __name__ == "__main__":
    main()
