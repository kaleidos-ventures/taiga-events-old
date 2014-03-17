import asyncio
import json
import traceback

from collections import namedtuple

from tornado.websocket import WebSocketHandler

from . import repository as repo
from . import signing
from . import classloader
from . import protocols as protos
from . import types

## Basic client/server protocol parsing functions.

def parse_auth_message(secret_key:str, message:str) -> types.AuthMsg:
    """
    Given a secret_key and raw auth message, parse it and
    return an instance of `types.AuthMsg`.
    """

    data = deserialize_data(message)

    # Common data validation
    assert "token" in data, "handshake message should contain token"
    assert "sessionId" in data, "handshake message should contain sessionId"

    token_data = signing.loads(data["token"], key=secret_key)
    return types.AuthMsg(data["token"], token_data["user_id"], data["sessionId"])


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
def build_subscription_patterns(appconf:types.AppConf, auth_msg:types.AuthMsg):
    main_repo = yield from repo.get_repository(appconf)
    projects = yield from repo.get_user_project_id_list(main_repo, auth_msg.user_id)
    return {"project.{}".format(x):1 for x in projects}


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

def is_same_session(identity, message):
    current_session_id = identity.session_id
    message_session_id = message.get("session_id", None)

    if message_session_id is None:
        return False

    return (current_session_id == message_session_id)


@asyncio.coroutine
def subscribe(wsconn:protos.WebSocketConnectionProtocol,
              appconf:types.AppConf,
              authmsg:str):
    """
    Given a web socket connection, and application config,
    start forwarding messages from queue broker backend to
    connected client matching a subscription.
    """
    assert isinstance(wsconn, protos.WebSocketConnectionProtocol)
    assert isinstance(appconf, types.AppConf)

    # Load configured implementation for queues
    queues = classloader.load_queue_implementation(appconf)
    subscription = None

    try:
        identity = parse_auth_message(appconf.secret_key, authmsg)
        patterns = yield from build_subscription_patterns(appconf, identity)

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
                wsconn.write(serialize_data(msg_data))

    except Exception as e:
        # In any error, write error message
        # and close the web sockets connection.

        # Websocket connection can raise an other exception
        # when trying send message throught closed connection.
        # This try/except ignores these exceptions.

        try:
            wsconn.write(serialize_error(e))
            wsconn.close()
        except Exception as e:
            pass

    finally:
        if subscription:
            yield from queues.close_subscription(subscription)
