import asyncio
from collections import namedtuple

from .utils import pg

Repository = namedtuple("Repository", ["connection", "vendor"])

@asyncio.coroutine
def get_repository(config:dict) -> Repository:
    """
    Given a generic config object, return a new
    `Storage` instance. Is a database connection
    abstraction.
    """
    connection = yield from pg.connect(**config)
    return Storage(connection, "postgresql")

@asyncio.coroutine
def user_is_in_project(repo:Repository, userid:int, projectid:int) -> bool:
    """
    Given storage, userid and projectid, test if
    a user with userid is member of project with
    projectid.
    """
    # TODO: refactor in next iteration for
    # complete multidatabase abstraction support.

    # This should work with mariadb but at this
    # momment it only supports postgresql
    assert repo.vendor == "postgresql"

    sql = ("select * from projects_membership where "
           "project_id = %s and user_id = %s;")

    with repo.connection.cursor() as cur:
        yield from cur.execute(sql, [projectid, userid])
        return len(cur.fetchall()) > 0
