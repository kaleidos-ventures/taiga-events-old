import asyncio
import argparse
import copy
import sys

# Install asyncio loop integration with tornado
from tornado.platform.asyncio import AsyncIOMainLoop
AsyncIOMainLoop().install()

from tornado.web import Application
from .handlers import MainHandler


DEFAULT_CONFIG = {
    "debug": True,
    "queue_conf": None,
    "repo_conf": None,
}


def make_app(config:dict) -> Application:
    application = Application([(r"/events", MainHandler)], debug=config["debug"])
    application.secret_key = config["secret_key"]
    application.repo_conf = config["repo_conf"]
    application.queue_conf = config["queue_conf"]
    return application


def start_app(application:Application, *, port:int=8888, join:bool=True):
    application.listen(port)
    print("Now listening on: http://127.0.0.1:{0}".format(port), file=sys.stderr)

    if join:
        loop = asyncio.get_event_loop()
        loop.run_forever()


def parse_config_file(path:str) -> dict:
    """
    Given a path to a config file return a parsed
    configuration as python dict.

    A configuration file consists in a simple python
    file containing global variables.
    """

    with open(path) as f:
        module = copy.copy(DEFAULT_CONFIG)
        exec(f.read(), {}, module)
        return module


def validate_config(config:dict) -> (bool, str):
    if not config.get("repo_conf", None):
        return False, "Repo configuration is empty"

    if not config.get("queue_conf", None):
        return False, "Queue configuration is empty"

    return True, None


def apply_args_to_config(config:dict, args) -> dict:
    config = copy.deepcopy(config)

    if args.debug is not None:
        config["debug"] = args.debug

    return config


def main():
    parser = argparse.ArgumentParser(description='Taiga.io events-consumer gateway.')
    parser.add_argument("-p", "--port", dest="port", action="store", type=int,
                        default=8888, help="Set custom port number.")
    parser.add_argument("-d", "--debug", dest="debug", action="store_true",
                        default=None, help="Run with debug mode activeted")
    parser.add_argument("-f", "--config", dest="configfile", action="store",
                        help="Read configuration from python config file", required=True)

    args = parser.parse_args()
    config = parse_config_file(args.configfile)
    ok, msg = validate_config(config)

    if not ok:
        print(msg, file=sys.stderr)
        return -1
    else:
        app = make_app(apply_args_to_config(config, args))
        return start_app(app, port=args.port)
