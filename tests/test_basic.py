# -*- coding: utf-8 -*-

import asyncio

from unittest.mock import patch
from unittest.mock import MagicMock

import pytest

from taiga_events import app
from taiga_events import types
from taiga_events import repository
from taiga_events import signing


def test_parse_auth_message():
    secret_key = "mysecret"

    token_data = {"token": signing.dumps({"user_id": 1}, key=secret_key),
                  "project": 1}

    auth_msg = app.parse_auth_message(secret_key, app.serialize_data(token_data))

    assert isinstance(auth_msg, types.AuthMsg)
    assert auth_msg.token == token_data["token"]
    assert auth_msg.user_id == 1
    assert auth_msg.project_id == 1


def test_is_subscription_allowed():
    mock_is_subscription_allowed = asyncio.coroutine(MagicMock(return_value=True))
    app_conf = types.AppConf("secretkey", None, None)

    token_data = {"token": signing.dumps({"user_id": 1}, key=app_conf.secret_key),
                  "project": 1}

    serialized_msg = app.serialize_data(token_data)

    with patch.object(app, "is_subscription_allowed", mock_is_subscription_allowed):
        coro = app.authenticate(app_conf, serialized_msg)

        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(coro)

        assert isinstance(result, types.AuthMsg)
        assert result.token == token_data["token"]

