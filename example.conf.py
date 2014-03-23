secret_key = "mysecret"

repo_conf = {
    "kwargs": {"dsn": "dbname=taiga"}
}

queue_conf = {
    "path": "taiga_events.queues.pg.EventsQueue",
    "kwargs": {
        "dsn": "dbname=taiga"
    }
}
