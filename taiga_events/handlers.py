import asyncio
import json
from collections import namedtuple

from tornado.websocket import WebSocketHandler

from . import queues
from . import repository as repo
from . import signing
from .exceptions import InternalException

## Custom types definition

AppConf = namedtuple("AppConf", ["secret_key", "repo_conf", "broker_conf"])
AuthMsg = namedtuple("AuthMsg", ["token", "user_id", "project_id"])

## Basic client/server protocol parsing functions.

def parse_auth_message(secret_key:str, message:str) -> AuthMsg:
    data = json.loads(message)

    # Common data validation
    assert "token" in data, "handshake message should contain token"
    assert "project" in data, "handshake message should contain project"

    try:
        token_data = signing.loads(data["token"], key=secret_key)
    except Exception as e:
        return InternalException(str(e))

    return AuthMsg(data["token"], token_data["user_id"], data["project"])


def serialize_data(data:dict) -> str:
    return json.dumps(data)


def serialize_error(error:Exception) -> str:
    return serialize_data({"error": str(error)})


## Main logic

@asyncio.coroutine
def is_subscription_allowed(repoconf:dict, authmsg:Message) -> bool:
    """
    Given a repoconf and parsed authentication message
    instance, and check if it can do make a subscription.
    """
    main_repo = yield from repo.get_repository(repoconf)
    is_allowed = yield from repo.user_is_in_project(main_repo,
                                                    authmsg.user_id
                                                    authmsg.project_id)
    return is_allowed


@asyncio.coroutine
def authenticate(conf:AppConf, raw_message:str) -> AuthMessage:
    """
    Given a appconf and first raw message that works
    as events handshake, try authenticate and test if
    client user cans subscribe to events or not.
    """
    repoconf, secret_key = conf.repoconf, conf.secret_key
    auth_msg = parse_auth_message(secret_key, raw_message)

    subscription_allowed = yield from is_subscription_allowed(repoconf, auth_msg)
    if not subscription_allowed:
        raise InternalException("subscription not allowed")
    return auth_msg


@asyncio.coroutine
def build_subscription_pattern(auth_msg:AuthMsg):
    pass


@asyncio.coroutine
def subscribe(wsconn, app_conf, stop_event):
    """
    Given a web socket connection, AppConf and asyncio stop
    event, forwards messages from broker to web sockets
    matching a subscription.

    :param wsconn: Subscription abstraction over WebSockets.

    It can works with any other abstractions like SockJs
    or SocketIO. The unique restriction is that `wsconn` should
    have the following interface:

    - write_message(message)
    - close()

    # TODO: make a good abstraction using abc.ABCMeta (?)
    """
    try:
        # Authenticate the first message.
        # This function can raise exception if authentication is failed.
        auth_msg = yield from authenticate(app_conf, message)

        sub_pattern = yield from build_subscription_pattern(auth_msg)
        subscription = yield from queues.subscribe(url=app_conf.broker_conf["url"],
                                                   pattern=sub_pattern)
        while not self.stop_event.is_set():
            message = yield from queues.consume_message(subscription)
            wsconn.write_message(message)

    except (InternalException, AssertionError) as e:
        wsconn.write_message(serialize_error(e))
        wsconn.close()

    finally:
        yield from queues.unsubscribe(subscription)


## Handler definition

class MessageSubscriptionHandlerMixin(object):
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

    This mixin sets:
    - subscribe(message): coroutine method for subscription.
    - stop_event: asyncio event for control client close event.
    """

    def initialize(self):
        self.stop_event = asyncio.Event()

    def on_message(self, raw_message):
        app_conf = AppConf(self.application.private_key,
                           self.application.repo_conf,
                           self.application.broker_conf)

        coro = subscribe(self, app_conf, self.stop_event)
        asyncio.async(coro)

    def on_close(self):
        self.stop_event.set()


class MainHandler(MessageSubscriptionHandlerMixin, WebSocketHandler):
    """
    Basic tornado handler for raw websocket hadling.
    """
