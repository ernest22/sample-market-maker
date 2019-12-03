import hashlib
import hmac
import json
import time
import urllib

from websocket import create_connection

###
# websocket-apikey-auth-test.py
#
# Reference Python implementation for authorizing with websocket.
# See https://www.bitmex.com/app/wsAPI for more details, including a list
# of methods.
###

# These are not real keys - replace them with your keys.
API_KEY = "ZFSng3vL3E64ltoemsPSpvyTGv-oaAP1z2d-t3sJ"
API_SECRET = "8ukwAK7WTFFBO_fuqZsgZXFXEJ7cQkBO5SZT5sLa"

# Switch these comments to use testnet instead.
BITMEX_URL = "wss://ftx.com"


VERB = "GET"
ENDPOINT = "/ws/"


def main():
    """Authenticate with the BitMEX API & request account information."""
    test_with_message()
    #test_with_querystring()


def test_with_message():
    # This is up to you, most use microtime but you may have your own scheme so long as it's increasing
    # and doesn't repeat.
    expires = int(time.time()) + 5
    # See signature generation reference at https://www.bitmex.com/app/apiKeys


    # Pinging Server
    #ws = create_connection(BITMEX_URL + ENDPOINT)
    ws = create_connection("wss://ftx.com/ws?subscribe=channel:trades,market:BTCUSD")
    result = ws.recv()
    print("Pinging Server")
    request = {"op": "ping"}
    ws.send(json.dumps(request))
    result = ws.recv()
    print("Received '%s'" % result)

    # Send API Key with signed message.
    #request = {"op": "authKeyExpires", "args": [API_KEY, expires, signature]}
    ts = int(time.time() * 1000)
    #signature = bitmex_signature(API_SECRET, VERB, ENDPOINT, expires)
    signature = hmac.new(API_SECRET.encode(), f'{ts}websocket_login'.encode(), 'sha256').hexdigest()
    request = {'op': 'login', 'args': {'key': API_KEY, 'sign': signature, 'time': ts, 'subaccount': "GBT"}}
    ws.send(json.dumps(request))
    print("Sent Auth request")
    #result = ws.recv()
    #print("Received '%s'" % result)

    # Send a request that requires authorization.
    request = {'op': 'subscribe', 'channel': 'orderbook', 'market': 'BTC-PERP'}
    ws.send(json.dumps(request))
    print("Sent subscribe")
    result = ws.recv()
    print("Received '%s'" % result)

    
    result = ws.recv()
    print("Received '%s'" % result)

    request = {'op': 'unsubscribe', 'channel': 'orderbook', 'market': 'BTC-PERP'}
    ws.send(json.dumps(request))
    print("Sent unsubscribe")
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
