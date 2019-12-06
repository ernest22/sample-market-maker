import sys
import websocket
import threading
import traceback
import ssl
from time import sleep
import hmac
import json
import time
import decimal
import logging
from market_maker.settings import settings
from market_maker.auth.APIKeyAuth import generate_expires, generate_signature
from market_maker.utils.log import setup_custom_logger
from market_maker.utils.math import toNearest
from future.utils import iteritems
from future.standard_library import hooks
from websocket import create_connection
with hooks():  # Python 2/3 compat
    from urllib.parse import urlparse, urlunparse

# Connects to BitMEX websocket for streaming realtime data.
# The Marketmaker still interacts with this as if it were a REST Endpoint, but now it can get
# much more realtime data without heavily polling the API.
#
# The Websocket offers a bunch of data as raw properties right on the object.
# On connect, it synchronously asks for a push of all this data then returns.
# Right after, the MM can start using its data. It will be updated in realtime, so the MM can
# poll as often as it wants.

class FTXWebsocket():

    # Don't grow a table larger than this amount. Helps cap memory usage.
    MAX_TABLE_LEN = 200

    def __init__(self):
        self.logger = logging.getLogger('root')
        self.__reset()

    def __del__(self):
        self.exit()

    def connect(self, endpoint="", symbol="BTCUSD", shouldAuth=True):
        '''Connect to the websocket and initialize data stores.'''

        self.logger.debug("Connecting WebSocket.")
        self.symbol = symbol
        self.shouldAuth = shouldAuth

        # We can subscribe right in the connection querystring, so let's build that.
        # Subscribe to all pertinent endpoints
        subscriptions = [sub + ':' + symbol for sub in ["quote", "trade"]]
        subscriptions += ["instrument"]  # We want all of them
        if self.shouldAuth:
            subscriptions += [sub + ':' + symbol for sub in ["order", "execution"]]
            subscriptions += ["margin", "position"]

        # Get WS URL and connect.

        urlParts = list(urlparse(endpoint))
        urlParts[0] = urlParts[0].replace('https', 'wss')
        urlParts[2] = "/ws"
        #urlParts[2] = "/ws?subscribe=" + ",".join(subscriptions)
        wsURL = urlunparse(urlParts)
        self.logger.info("Connecting to %s" % wsURL)
        self.__connect(wsURL)
        self.logger.info('Connected to FTX WS. Waiting for data images, this may take a moment...')
        self.__send_command('subscribe', 'orderbook', self.symbol)
        # Connected. Wait for partials
        self.__wait_for_symbol(symbol)
        self.__auth()
        self.__send_privcommand('subscribe', 'fills')
        self.__send_privcommand('subscribe', 'orders')
        self.logger.info('Got all market data. Starting.')

    #
    # Data methods
    #
    def get_instrument(self, symbol):
        #print("get_instrument")
        #print(self.data)
        instruments = self.data['market']
        matchingInstruments = [i for i in instruments if i['symbol'] == symbol]
        if len(matchingInstruments) == 0:
           raise Exception("Unable to find instrument or index with symbol: " + symbol)
        instrument = matchingInstruments[0]
        # Turn the 'tickSize' into 'tickLog' for use in rounding
        # http://stackoverflow.com/a/6190291/832202
        instrument['tickLog'] = decimal.Decimal(str(instrument['tickSize'])).as_tuple().exponent * -1
        return instrument


    def get_ticker(self, symbol):
        '''Return a ticker object. Generated from instrument.'''

        instrument = self.get_instrument(symbol)
        # If this is an index, we have to get the data from the last trade.
        if instrument['symbol'][0] == '.':
            ticker = {}
            ticker['mid'] = ticker['buy'] = ticker['sell'] = ticker['last'] = instrument['markPrice']
        # Normal instrument
        else:
            bid = instrument['bidPrice'] or instrument['lastPrice']
            ask = instrument['askPrice'] or instrument['lastPrice']
            ticker = {
                "last": instrument['lastPrice'],
                "buy": bid,
                "sell": ask,
                "mid": (bid + ask) / 2
            }

        # The instrument has a tickSize. Use it to round values.
        return {k: toNearest(float(v or 0), instrument['tickSize']) for k, v in iteritems(ticker)}

    def funds(self):
        return self.data['margin'][0]

    def market_depth(self, symbol):
        #raise NotImplementedError('orderBook is not subscribed; use askPrice and bidPrice on instrument')
        orderbook = {
            "bids" : self.data['orderbook'][2],
            "asks" : self.data['orderbook'][3]
        }
        return orderbook

    def open_orders(self, clOrdIDPrefix):
        orders = self.data['order']
        # Filter to only open orders (leavesQty > 0) and those that we actually placed
        return [o for o in orders if str(o['clOrdID']).startswith(clOrdIDPrefix) and o['leavesQty'] > 0]

    def position(self, symbol):
        positions = self.data['position']
        pos = [p for p in positions if p['symbol'] == symbol]
        if len(pos) == 0:
            # No position found; stub it
            return {'avgCostPrice': 0, 'avgEntryPrice': 0, 'currentQty': 0, 'symbol': symbol}
        return pos[0]

    def recent_trades(self):
        return self.data['trade']

    #
    # Lifecycle methods
    #
    def error(self, err):
        self._error = err
        self.logger.error(err)
        self.exit()

    def exit(self):
        self.exited = True
        self.ws.close()

    #
    # Private methods
    #

    def __connect(self, wsURL):
        '''Connect to the websocket in a thread.'''
        self.logger.debug("Starting thread")

        ssl_defaults = ssl.get_default_verify_paths()
        sslopt_ca_certs = {'ca_certs': ssl_defaults.cafile}
        self.ws = websocket.WebSocketApp(wsURL,
                                         on_message=self.__on_message,
                                         on_close=self.__on_close,
                                         on_open=self.__on_open,
                                         on_error=self.__on_error,
                                         #header=self.__auth()
                                         )

        setup_custom_logger('websocket', log_level=settings.LOG_LEVEL)
        self.wst = threading.Thread(target=lambda: self.ws.run_forever(sslopt=sslopt_ca_certs))
        self.wst.daemon = True
        self.wst.start()
        self.logger.info("Started thread")

        # Wait for connect before continuing
        conn_timeout = 5
        while (not self.ws.sock or not self.ws.sock.connected) and conn_timeout and not self._error:
            sleep(1)
            conn_timeout -= 1

        if not conn_timeout or self._error:
            self.logger.error("Couldn't connect to WS! Exiting.")
            self.exit()
            sys.exit(1)

    def __auth(self):
        '''Return auth headers. Will use API Keys if present in settings.'''

        self.logger.info("Authenticating with API Key.")
        # To auth to the WS using an API key, we generate a signature of a nonce and
        # the WS API endpoint.
        ts = int(time.time() * 1000)
        signature = hmac.new(settings.FTXAPI_SECRET.encode(), f'{ts}websocket_login'.encode(), 'sha256').hexdigest()
        request = {'op': 'login', 'args': {'key': settings.FTXAPI_KEY, 'sign': signature, 'time': ts, 'subaccount': settings.FTXAcc}}
        self.ws.send(json.dumps(request))

    def __wait_for_account(self):
        '''On subscribe, this data will come down. Wait for it.'''
        # Wait for the keys to show up from the ws
        
        while not {'margin', 'position', 'order'} <= set(self.data):
            sleep(0.1)

    def __wait_for_symbol(self, symbol):
        '''On subscribe, this data will come down. Wait for it.'''
        while not {'orderbook'} <= set(self.data):
            sleep(0.1)

    def __send_command(self, command, table, market):
        '''Send a raw command.'''
        self.ws.send(json.dumps({"op": command, "channel": table, "market": market}))

    def __send_privcommand(self, command, table):
        '''Send a raw private command.'''
        self.ws.send(json.dumps({"op": command, "channel": table}))

    def __on_message(self, message):
        '''Handler for parsing WS messages.'''
        message = json.loads(message)
        self.logger.debug(json.dumps(message))

        table = message['channel'] if 'channel' in message else None
        action = message['data']['action'] if 'data' in message and 'action' in message['data'] else None
        try:
            if 'subscribed' in message['type']:
                if 'market' in message:
                    self.logger.debug("Subscribed to %(channel)s %(market)s."  % { "channel": message['channel'],  "market": message['market']})
                else:
                    self.logger.debug("Subscribed to %s."  % message['channel'])
            elif 'unsubscribed' in message['type']:
                self.logger.debug("Unsubscribed to %s." % message['market'])
            elif 'info' in message['type']:
                if message['code'] == 20001:
                    self.error("Please check and restart.")
                self.logger.debug("Info Code: " + message['code'] + "Message: " + message['msg'])
            elif 'error' in message['type']:
                if message['status'] == 400:
                    self.error(message['error'])
                if message['status'] == 401:
                    self.error("API Key incorrect, please check and restart.")
            elif action:
                if table not in self.data:
                    self.data[table] = []
                
                
                # There are four possible types from the WS:
                # 'partial' - full table image
                # 'insert'  - new row
                # 'update'  - update row
                # 'delete'  - delete row
                #print(message)
                if 'partial' in message['data']['action']:
                    self.logger.debug("%s: partial" % table)
                    datalist = list(message['data'].values()) 
                    self.data[table] += datalist


                if 'update' in message['data']['action']:
                    
                    self.logger.debug('%s: updating %s' % (table, message['data']))
                    
                    # Locate the item in the collection and update it.
                    updateDatalist = list(message['data'].items()) 
                    for updateData in updateDatalist:
                        if(updateData[0] == 'asks' or updateData[0] == 'bids'):
                            if(updateData[1] == []):
                                continue
                            elif(updateData[0] == 'asks'):
                                for bookupdate in updateData[1]:
                                    i = 0
                                    for listitem in self.data[table][3]:
                                        length = len(self.data[table][3])  
                                        if(self.data[table][3][i][0] == bookupdate[0]):      
                                            self.data[table][3][i] = bookupdate
                                            if(bookupdate[1] == 0):
                                                self.data[table][3].remove(bookupdate)
                                                break  
                                            break
                                        elif(float(self.data[table][3][i][0]) > float(bookupdate[0])):  
                                            self.data[table][3].insert(i, bookupdate)   
                                            break
                                        if(i == length-1):
                                            if(bookupdate[1] == 0):
                                                break
                                            self.data[table][3].append(bookupdate)   
                                            break
                                        i += 1
                            elif(updateData[0] == 'bids'):
                                for bookupdate in updateData[1]:
                                    i = 0
                                    for listitem in self.data[table][2]:  
                                        length = len(self.data[table][2]) 
                                        if(self.data[table][2][i][0] == bookupdate[0]):
                                            self.data[table][2][i] = bookupdate
                                            if(bookupdate[1] == 0):
                                                self.data[table][2].remove(bookupdate)
                                            break
                                        elif(float(self.data[table][2][i][0]) < float(bookupdate[0])):  
                                            self.data[table][2].insert(i, bookupdate)   
                                            break
                                        if(i == length-1):
                                            if(bookupdate[1] == 0):
                                                break
                                            self.data[table][2].append(bookupdate)   
                                            break
                                        i += 1


                        
                        else:
                            continue  # No item found to update. Could happen before push

                        # Log executions

            elif table == 'orders':
                if table not in self.data:
                    self.data[table] = []
                updateData = list(message['data'].values()) 
                if(updateData[7] == "closed"):
                    id = updateData[0]
                    i = 0
                    for order in self.data[table]:
                        if(order[0] == id):
                            self.data[table].pop(i)
                            break
                        i += 1
                else:
                    id = updateData[0]
                    i = 0
                    NewOrder = True
                    for order in self.data[table]:
                        if(order[0] == id):
                            self.data[table][i] = updateData
                            NewOrder = False
                            break
                        i += 1
                    if(NewOrder == True):
                        self.data[table].append(updateData) 
                        # Remove canceled / filled orders
            elif table == 'fills':
                if table not in self.data:
                    self.data[table] = []
                updateData = list(message['data'].values()) 
                self.data[table].append(updateData)     
                print(message['data'].values())
                #logger.info('FTX Fill Price: %(fill)s ' % {"fill": message['data']})
            else:
                raise Exception("Unknown type: %s" % action)
        except:
            self.logger.error(traceback.format_exc())

    def __on_open(self):
        self.logger.debug("Websocket Opened.")

    def __on_close(self):
        self.logger.info('Websocket Closed')
        self.exit()

    def __on_error(self, ws, error):
        if not self.exited:
            self.error(error)

    def __reset(self):
        self.data = {}
        self.exited = False
        self._error = None

class BKWebsocket():

    # Don't grow a table larger than this amount. Helps cap memory usage.
    MAX_TABLE_LEN = 200

    def __init__(self):
        self.logger = logging.getLogger('root')
        self.__reset()

    def __del__(self):
        self.exit()

    def connect(self, endpoint="", symbol="XBTN15", shouldAuth=True):
        '''Connect to the websocket and initialize data stores.'''

        self.logger.debug("Connecting WebSocket.")
        self.symbol = symbol
        self.shouldAuth = shouldAuth

        # We can subscribe right in the connection querystring, so let's build that.
        # Subscribe to all pertinent endpoints
        subscriptions = [sub + ':' + symbol for sub in ["quote", "trade"]]
        subscriptions += ["instrument"]  # We want all of them
        if self.shouldAuth:
            subscriptions += [sub + ':' + symbol for sub in ["order", "execution"]]
            subscriptions += ["margin", "position"]

        # Get WS URL and connect.
        '''
        urlParts = list(urlparse(endpoint))
        urlParts[0] = urlParts[0].replace('http', 'ws')
        urlParts[2] = "/realtime?subscribe=" + ",".join(subscriptions)
        '''
        wsURL = "wss://api.bitkub.com/websocket-api/market.trade.thb_btc"
        self.logger.info("Connecting to %s" % wsURL)
        self.__connect(wsURL)
        self.logger.info('Connected to Bitkub WS. Waiting for data images, this may take a moment...')

        # Connected. Wait for partials
        #self.__wait_for_symbol(symbol)
        #if self.shouldAuth:
        #    self.__wait_for_account()
        #self.logger.info('Got all market data. Starting.')

    #
    # Data methods
    #
    def get_instrument(self, symbol):
        instruments = self.data['instrument']
        matchingInstruments = [i for i in instruments if i['symbol'] == symbol]
        if len(matchingInstruments) == 0:
            raise Exception("Unable to find instrument or index with symbol: " + symbol)
        instrument = matchingInstruments[0]
        # Turn the 'tickSize' into 'tickLog' for use in rounding
        # http://stackoverflow.com/a/6190291/832202
        instrument['tickLog'] = decimal.Decimal(str(instrument['tickSize'])).as_tuple().exponent * -1
        return instrument

    def get_ticker(self, symbol):
        '''Return a ticker object. Generated from instrument.'''

        instrument = self.get_instrument(symbol)

        # If this is an index, we have to get the data from the last trade.
        if instrument['symbol'][0] == '.':
            ticker = {}
            ticker['mid'] = ticker['buy'] = ticker['sell'] = ticker['last'] = instrument['markPrice']
        # Normal instrument
        else:
            bid = instrument['bidPrice'] or instrument['lastPrice']
            ask = instrument['askPrice'] or instrument['lastPrice']
            ticker = {
                "last": instrument['lastPrice'],
                "buy": bid,
                "sell": ask,
                "mid": (bid + ask) / 2
            }

        # The instrument has a tickSize. Use it to round values.
        return {k: toNearest(float(v or 0), instrument['tickSize']) for k, v in iteritems(ticker)}

    def funds(self):
        return self.data['margin'][0]

    def market_depth(self, symbol):
        raise NotImplementedError('orderBook is not subscribed; use askPrice and bidPrice on instrument')
        # return self.data['orderBook25'][0]

    def open_orders(self, clOrdIDPrefix):
        orders = self.data['order']
        # Filter to only open orders (leavesQty > 0) and those that we actually placed
        return [o for o in orders if str(o['clOrdID']).startswith(clOrdIDPrefix) and o['leavesQty'] > 0]

    def position(self, symbol):
        positions = self.data['position']
        pos = [p for p in positions if p['symbol'] == symbol]
        if len(pos) == 0:
            # No position found; stub it
            return {'avgCostPrice': 0, 'avgEntryPrice': 0, 'currentQty': 0, 'symbol': symbol}
        return pos[0]

    def recent_trades(self):
        return self.data['trade']

    #
    # Lifecycle methods
    #
    def error(self, err):
        self._error = err
        self.logger.error(err)
        self.exit()

    def exit(self):
        self.exited = True
        self.ws.close()

    #
    # Private methods
    #

    def __connect(self, wsURL):
        '''Connect to the websocket in a thread.'''
        self.logger.debug("Starting thread")

        ssl_defaults = ssl.get_default_verify_paths()
        sslopt_ca_certs = {'ca_certs': ssl_defaults.cafile}
        self.ws = websocket.WebSocketApp(wsURL,
                                         on_message=self.__on_message,
                                         on_close=self.__on_close,
                                         on_open=self.__on_open,
                                         on_error=self.__on_error,
                                         header=self.__get_auth()
                                         )

        setup_custom_logger('websocket', log_level=settings.LOG_LEVEL)
        self.wst = threading.Thread(target=lambda: self.ws.run_forever(sslopt=sslopt_ca_certs))
        self.wst.daemon = True
        self.wst.start()
        self.logger.info("Started thread")

        # Wait for connect before continuing
        conn_timeout = 5
        while (not self.ws.sock or not self.ws.sock.connected) and conn_timeout and not self._error:
            sleep(1)
            conn_timeout -= 1

        if not conn_timeout or self._error:
            self.logger.error("Couldn't connect to WS! Exiting.")
            self.exit()
            sys.exit(1)

    def __get_auth(self):
        '''Return auth headers. Will use API Keys if present in settings.'''

        if self.shouldAuth is False:
            return []

        self.logger.info("Authenticating with API Key.")
        # To auth to the WS using an API key, we generate a signature of a nonce and
        # the WS API endpoint.
        nonce = generate_expires()
        return [
            "api-expires: " + str(nonce),
            "api-signature: " + generate_signature(settings.API_SECRET, 'GET', '/realtime', nonce, ''),
            "api-key:" + settings.API_KEY
        ]

    def __wait_for_account(self):
        '''On subscribe, this data will come down. Wait for it.'''
        # Wait for the keys to show up from the ws
        while not {'margin', 'position', 'order'} <= set(self.data):
            sleep(0.1)

    def __wait_for_symbol(self, symbol):
        '''On subscribe, this data will come down. Wait for it.'''
        while not {'instrument', 'trade', 'quote'} <= set(self.data):
            sleep(0.1)

    def __send_command(self, command, args):
        '''Send a raw command.'''
        self.ws.send(json.dumps({"op": command, "args": args or []}))

    def __on_message(self, message):
        '''Handler for parsing WS messages.'''
        message = json.loads(message)
        self.logger.debug(json.dumps(message))

        table = message['table'] if 'table' in message else None
        action = message['type'] if 'type' in message else None
        try:
            if 'subscribe' in message:
                if message['success']:
                    self.logger.debug("Subscribed to %s." % message['subscribe'])
                else:
                    self.error("Unable to subscribe to %s. Error: \"%s\" Please check and restart." %
                               (message['request']['args'][0], message['error']))
            elif 'status' in message:
                if message['status'] == 400:
                    self.error(message['error'])
                if message['status'] == 401:
                    self.error("API Key incorrect, please check and restart.")
            elif action:

                if table not in self.data:
                    self.data[table] = []


                # There are four possible types from the WS:
                # 'partial' - full table image
                # 'insert'  - new row
                # 'update'  - update row
                # 'delete'  - delete row
                if action == 'partial':
                    self.logger.debug("%s: partial" % table)
                    self.data[table] += message['data']
                    # Keys are communicated on partials to let you know how to uniquely identify
                    # an item. We use it for updates.
                elif action == 'insert':
                    self.logger.debug('%s: inserting %s' % (table, message['data']))
                    self.data[table] += message['data']

                    # Limit the max length of the table to avoid excessive memory usage.
                    # Don't trim orders because we'll lose valuable state if we do.
                    if table not in ['order', 'orderBookL2'] and len(self.data[table]) > BitMEXWebsocket.MAX_TABLE_LEN:
                        self.data[table] = self.data[table][(BitMEXWebsocket.MAX_TABLE_LEN // 2):]

                elif action == 'update':
                    self.logger.debug('%s: updating %s' % (table, message['data']))
                    # Locate the item in the collection and update it.
                    for updateData in message['data']:
                        item = findItemByKeys(self.data[table], updateData)
                        if not item:
                            continue  # No item found to update. Could happen before push

                        # Log executions
                        if table == 'order':
                            is_canceled = 'ordStatus' in updateData and updateData['ordStatus'] == 'Canceled'
                            if 'cumQty' in updateData and not is_canceled:
                                contExecuted = updateData['cumQty'] - item['cumQty']
                                if contExecuted > 0:
                                    instrument = self.get_instrument(item['symbol'])
                                    self.logger.info("Execution: %s %d Contracts of %s at %.*f" %
                                             (item['side'], contExecuted, item['symbol'],
                                              instrument['tickLog'], item['price']))

                        # Update this item.
                        item.update(updateData)

                        # Remove canceled / filled orders
                        if table == 'order' and item['leavesQty'] <= 0:
                            self.data[table].remove(item)

                elif action == 'delete':
                    self.logger.debug('%s: deleting %s' % (table, message['data']))
                    # Locate the item in the collection and remove it.
                    for deleteData in message['data']:
                        item = findItemByKeys(self.data[table], deleteData)
                        self.data[table].remove(item)
                else:
                    raise Exception("Unknown type: %s" % action)
        except:
            self.logger.error(traceback.format_exc())

    def __on_open(self):
        self.logger.debug("Websocket Opened.")

    def __on_close(self):
        self.logger.info('Websocket Closed')
        self.exit()

    def __on_error(self, ws, error):
        if not self.exited:
            self.error(error)

    def __reset(self):
        self.data = {}
        self.exited = False
        self._error = None


def findItemByKeys(table, matchData):
    for item in table[2][1]:
        matched = True
        if item[0] != matchData[0]:
            matched = False
        if matched:
            return item

if __name__ == "__main__":
    # create console handler and set level to debug
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # add formatter to ch
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    ws = FTXWebsocket()
    ws.logger = logger
    ws.connect("https://ftx.com/api/")
    while(ws.ws.sock.connected):
        sleep(1)

