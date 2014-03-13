from collections import namedtuple

AppConf = namedtuple("AppConf", ["secret_key", "repo_conf", "queue_conf"])
AuthMsg = namedtuple("AuthMsg", ["token", "user_id"])
