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

from .exceptions import InternalException

## Basic client/server protocol parsing functions.

def parse_auth_message(secret_key:str, message:str) -> types.AuthMsg:
    """
    Given a secret_key and raw auth message, parse it and
    return an instance of `types.AuthMsg`.
    """
    data = json.loads(message)

    # Common data validation
    assert "token" in data, "handshake message should contain token"
    assert "project" in data, "handshake message should contain project"

    try:
        token_data = signing.loads(data["token"], key=secret_key)
    except Exception as e:
        return InternalException(str(e))

    return types.AuthMsg(data["token"], token_data["user_id"], data["project"])


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
def is_subscription_allowed(appconf:types.AppConf, authmsg:types.AuthMsg) -> bool:
    """
    Given a repoconf and parsed authentication message
    instance, and check if it can do make a subscription.
    """
    assert isinstance(appconf, types.AppConf)
    assert isinstance(authmsg, types.AuthMsg)

    repo_conf = appconf.repo_conf
    main_repo = yield from repo.get_repository(repoconf)
    is_allowed = yield from repo.user_is_in_project(main_repo,
                                                    authmsg.user_id,
                                                    authmsg.project_id)
    return is_allowed


@asyncio.coroutine
def authenticate(appconf:types.AppConf, raw_message:str) -> types.AuthMsg:
    """
    Given a appconf and first raw message that works
    as events handshake, try authenticate and test if
    client user cans subscribe to events or not.
    """
    assert isinstance(appconf, types.AppConf)
    assert isinstance(raw_message, str)

    secret_key = appconf.secret_key
    auth_msg = parse_auth_message(secret_key, raw_message)

    subscription_allowed = yield from is_subscription_allowed(appconf, auth_msg)
    if not subscription_allowed:
        raise InternalException("subscription not allowed")
    return auth_msg


@asyncio.coroutine
def build_subscription_pattern(auth_msg:types.AuthMsg):
    return None


@asyncio.coroutine
def subscribe(wsconn:protos.WebSocketConnectionProtocol,
              appconf:types.AppConf):
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
        # Authenticate the first message.
        # This function can raise exception if authentication is failed.
        #auth_msg = yield from authenticate(appconf, message)

        # TODO: this at this momment does nothing
        sub_pattern = yield from build_subscription_pattern(None)

        # Create new subscription and run infinite loop
        # for consume messages.
        subscription = yield from queues.subscribe(sub_pattern)
        while True:
            message = yield from queues.consume_message(subscription)
            wsconn.write(message)

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
