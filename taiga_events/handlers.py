import asyncio
import json
import traceback

from . import repository as repo
from . import signing
from . import classloader
from . import types
from . import websocket as ws


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


@asyncio.coroutine
def parse_auth_message(config:dict, message:str) -> types.AuthMsg:
    """
    Parses first message received throught websocket
    connection (auth message).
    """
    data = deserialize_data(message)

    # Common data validation
    assert "token" in data, "handshake message should contain token"
    assert "sessionId" in data, "handshake message should contain sessionId"

    token_data = signing.loads(data["token"], key=config["secret_key"])
    return types.AuthMsg(data["token"], token_data["user_id"], data["sessionId"])


@asyncio.coroutine
def build_subscription_patterns(config:dict, auth_msg:types.AuthMsg) -> dict:
    conn = yield from repo.get_connection(config)
    projects = yield from repo.get_user_project_id_list(conn, auth_msg.user_id)
    return {"project.{}".format(x) for x in projects}


def match_message_with_patterns(message:str, patterns:dict) -> bool:
    """
    Given a message and patterns dict structure, try find
    a message routing_key on patterns. If it found return True,
    else returns False.
    """
    try:
        msg = deserialize_data(message)
    except Exception as e:
        return False

    if "routing_key" not in msg:
        return False

    routing_key = msg["routing_key"]
    return (routing_key in patterns)


def is_same_session(identity:types.AuthMsg, message:dict) -> bool:
    current_session_id = identity.session_id
    message_session_id = message.get("session_id", None)

    if message_session_id is None:
        return False
    return (current_session_id == message_session_id)


@asyncio.coroutine
def subscribe(config, ws, message):
    """
    Given a web socket connection, and application config,
    start forwarding messages from queue broker backend to
    connected client matching a subscription.
    """
    # Load configured implementation for queues
    queues = classloader.load_queue_implementation(config)
    subscription = None

    try:
        identity = yield from parse_auth_message(config, message)
        patterns = yield from build_subscription_patterns(config, identity)

        # Create new subscription and run infinite loop
        # for consume messages.
        subscription = yield from queues.subscribe()

        while True:
            msg = yield from queues.consume_message(subscription)
            msg_data = deserialize_data(msg)

            # Filter messages from same session
            if is_same_session(identity, msg_data):
                continue

            if match_message_with_patterns(msg, patterns):
                ws.write(serialize_data(msg_data))

    except Exception as e:
        # In any error, write error message
        # and close the web sockets connection.

        # Websocket connection can raise an other exception
        # when trying send message throught closed connection.
        # This try/except ignores these exceptions.
        print("DEBUG1:", type(e), e)

        try:
            ws.write(serialize_error(e))
            ws.close()
        except Exception as e:
            print("DEBUG2:", type(e), e)
            pass

    finally:
        if subscription:
            yield from queues.close_subscription(subscription)


class EventsHandler(ws.WebSocketHandler):
    """
    This handler wants receive first message
    containing: authentication token, project id and
    object type.

    Simple example of received data:

    {"token": "1233456789qwertyyuuoip",
     "project": "1"}

    If received token is invalid or user is not member
    of any project, websocket is closed.
    """

    def on_initialize(self, config:dict):
        self.config = config

    def on_open(self, ws):
        self.queues = classloader.load_queue_implementation(self.config)
        self.t = None

    def on_message(self, ws, message):
        sub = subscribe(self.config, ws, message)
        self.t = asyncio.async(sub)

    def on_close(self, ws):
        if not self.t:
            return

        self.t.cancel()
        self.t = None
