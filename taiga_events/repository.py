import asyncio
from collections import namedtuple

from . import types
from .utils import pg

Connection = namedtuple("Repository", ["connection", "vendor"])

@asyncio.coroutine
def get_connection(conf:dict) -> Connection:
    repo_conf = conf["repo_conf"]
    connection = yield from pg.connect(**repo_conf["kwargs"])
    return Connection(connection, "postgresql")


@asyncio.coroutine
def get_user_project_id_list(repo:Connection, user_id:int) -> [int]:
    """
    Given an repository instance and user id, return all project
    id's associated with that user.
    """

    assert repo.vendor == "postgresql"
    sql = ("select project_id from projects_membership "
           "where user_id = %s;")

    with repo.connection.cursor() as cur:
        yield from cur.execute(sql, [user_id])
        return [x[0] for x in cur.fetchall()]
