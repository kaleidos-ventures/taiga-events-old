import asyncio
import json
from collections import namedtuple

from tornado.websocket import WebSocketHandler
from .consumer import subscribe, unsubscribe, consume_message

## Custom types definition

Message = namedtuple("ClientMessage", ["token", "project_id", "object_type"])

class InternalError(Exception):
    pass

## Basic client/server protocol parsing functions.

def parse_auth_message(message):
    data = json.loads(message)
    return Message(data["token"], data["project"], data["type"])

def serialize(data):
    return json.dumps(data)

def serialize_error(error):
    return serialize({"error": error})

## Handler definition

class MainHandler(WebSocketHandler):
    """
    This handler wants receive first message
    containing: authentication token, project id and
    object type.

    Simple example of received data:

    {"token": "1233456789qwertyyuuoip",
     "project": "1",
     "type": "issue"}

    If received token is invalid or user is not member
    of any project, websocket is closed.
    """

    def initialize(self):
        self.broker_url = self.application.broker_url
        self.dbconf = self.application.dbconf
        self.event = asyncio.Event()

    @asyncio.coroutine
    def authenticate(self, raw_message):
        try:
            auth_msg = parse_auth_message(raw_message)
        except Exception as e:
            raise InternalError(str(e))

        # TODO: check if user is part of project
        return auth_msg

    @asyncio.coroutine
    def subscribe(self, message):
        try:
            auth = yield from self.authenticate(message)
            route_key = yield from self.build_route_key(auth)
            subscription = yield from subscribe(url=self.broker_url)

            while not self.event.is_set():
                message = yield from consume_message(subscription)
                self.write_message(message)

        except InternalError as e:
            self.write(serialize_error(e))
            self.close()

        finally:
            yield from unsubscribe(subscription)

    def on_message(self, message):
        asyncio.async(self.subscribe(message))

    def on_close(self):
        self.event.set()
