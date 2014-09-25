import asyncio
import json
import traceback
import logging

from . import repository as repo
from . import signing
from . import classloader as loader
from . import types
from . import websocket as ws

log = logging.getLogger("taiga")


def deserialize_data(data:str) -> dict:
    """
    Given a string with json, return
    a deserialized python representation
    of it.
    """
    return json.loads(data)


def serialize_data(data:dict) -> str:
    """
    Given a python native data type,
    serialize it to json.
    """
    return json.dumps(data)


def serialize_error(error:Exception) -> str:
    """
    Given any exception, serialize it
    to json with default serialization method.
    """
    return serialize_data({"error": str(error)})


def is_same_session(identity:types.AuthMsg, message:dict) -> bool:
    current_session_id = identity.session_id
    message_session_id = message.get("session_id", None)

    if message_session_id is None:
        return False
    return (current_session_id == message_session_id)


class Subscription(object):
    def __init__(self, identity, routing_key, queues, ws):
        self.identity = identity
        self.queues = queues
        self.routing_key = routing_key
        self.ws = ws

        self.loop = None

    @asyncio.coroutine
    def start(self):
        self.loop = asyncio.Task(self._subscription_ventilator())

    @asyncio.coroutine
    def stop(self):
        if not self.loop:
            return
        self.loop.cancel()

    @asyncio.coroutine
    def _subscription_ventilator(self):
        queues = self.queues
        sub = yield from queues.subscribe(self.routing_key)

        try:
            while True:
                msg = yield from queues.consume_message(sub)
                log.debug("Received message for %s: [%s] - %s",
                          self.ws.remote_ip, self.routing_key, msg)

                if is_same_session(self.identity, msg):
                    # Excplicit context switch
                    yield from asyncio.sleep(0)
                    continue

                msg["routing_key"] = self.routing_key
                msg = json.dumps(msg)
                self.ws.write(msg)

        except asyncio.CancelledError:
            # Raised when connection is closed from browser
            # side. Nothing todo in this case.
            log.debug("Subscription canceled %s by %s", self.routing_key, self.ws.remote_ip,
                      exc_info=False, stack_info=False)

        except Exception as e:
            # In any error, write error message
            # and close the web sockets connection.

            # Websocket connection can raise an other exception
            # when trying send message throught closed connection.
            # This try/except ignores these exceptions.
            log.error("Unhandled exception", exc_info=True, stack_info=False)

            try:
                self.ws.write(serialize_error(e))
                self.ws.close()
            except Exception as e:
                log.error("Unhandled exception", exc_info=True, stack_info=False)

        yield from queues.close_subscription(sub)


class ConnectionHandler(object):
    def __init__(self, ws, config):
        self.ws = ws
        self.config = config
        self.authenticated = False
        self.subscriptions = {}
        self.queues = loader.load_queue_implementation(config)

    @asyncio.coroutine
    def close(self):
        # Closed all subscriptions
        for name, item in self.subscriptions.items():
            yield from item.stop()

        # self.queues.close_all_connections()

    @asyncio.coroutine
    def parse_auth_message(self, message:dict) -> types.AuthMsg:
        """
        Parses first message received throught websocket
        connection (auth message).
        """
        assert "token" in message, "handshake message should contain token"
        assert "sessionId" in message, "handshake message should contain sessionId"

        token_data = signing.loads(message["token"], key=self.config["secret_key"])
        return types.AuthMsg(message["token"], token_data["user_id"], message["sessionId"])

    @asyncio.coroutine
    def authenticate(self, message:dict):
        log.debug("Authenticating peer %s with: %s", self.ws.remote_ip, message)
        self.identity = yield from self.parse_auth_message(message)

    @asyncio.coroutine
    def add_subscription(self, routing_key):
        log.debug("Initializing subsciption to: {}".format(routing_key))

        subscription = Subscription(self.identity, routing_key, self.queues, self.ws)
        yield from subscription.start()
        self.subscriptions[routing_key] = subscription

    @asyncio.coroutine
    def remove_subscription(self, routing_key):
        if routing_key in self.subscriptions:
            subscription = self.subscriptions[routing_key]

            yield from subscription.stop()
            del self.subscriptions[routing_key]

    @asyncio.coroutine
    def add_message(self, message):
        cmd = message.get("cmd", None)

        if cmd == "auth":
            authdata = message.get("data")
            yield from self.authenticate(authdata)
            self.authenticated = True
        else:
            yield from self.handle_message(message)

    @asyncio.coroutine
    def handle_message(self, message:dict):
        if not self.authenticated:
            log.info("Unathenticated message from %s: %s", self.ws.remote_ip, message)
            return

        cmd = message.get("cmd", None)

        if cmd == "subscribe":
            routing_key = message.get("routing_key", None)
            yield from self.add_subscription(routing_key)
        elif cmd == "unsubscribe":
            routing_key = message.get("routing_key", None)
            yield from self.remove_subscription(routing_key)
        else:
            log.warning("Received unexpected message from %s: %s", self.ws.remote_ip, message)


class EventsHandler(ws.WebSocketHandler):
    def on_initialize(self, config:dict):
        self.config = config

    def on_open(self, ws):
        log.debug("Websocket connection opened from %s", ws.remote_ip)
        self.t = ConnectionHandler(ws, self.config)

    def on_message(self, ws, message):
        log.debug("Websocket message received from %s: %s", ws.remote_ip, message)
        asyncio.Task(self.t.add_message(json.loads(message)))

    def on_close(self, ws):
        log.debug("Websocket connection closed from %s", ws.remote_ip)
        asyncio.Task(self.t.close())
