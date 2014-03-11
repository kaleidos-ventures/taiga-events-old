# -*- coding: utf-8 -*-

from unittest.mock import patch
from unittest.mock import MagicMock

import pytest

from taiga_events.handlers import *
from taiga_events import repository
from taiga_events import signing


def test_parse_auth_message():
    secret_key = "mysecret"

    token_data = {"token": signing.dumps({"user_id": 1}, key=secret_key),
                  "project": 1}

    auth_msg = parse_auth_message(secret_key, serialize_data(token_data))

    assert isinstance(auth_msg, AuthMsg)
    assert auth_msg.token == token_data["token"]
    assert auth_msg.user_id == 1
    assert auth_msg.project_id == 1

# def test_is_subscription_allowed():
#     repo = repository.Repository(None, None)
#     mock_get_repository = asyncio.coroutine(MagicMock(return_value=repo))
#     mock_user_is_in_project = asyncio.coroutine(MagicMock(return_value=True))
#     with patch.object(repo, "get_repository", mock_get_repository):

