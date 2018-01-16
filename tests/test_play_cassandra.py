#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `play_cassandra` package."""

import pytest


@pytest.fixture(scope='session')
def variables():
    return {'skins': {'skin1': {'base_url': 'http://', 'credentials': {}}}}


def test_provider():
    from play_cassandra import providers
    import mock
    provider = providers.CassandraProvider(None)
    assert provider.engine is None
    command = {
        'provider': 'play_cassandra',
        'type': 'execute',
        'connection': {
            'auth_provider': {
                'username': 'user',
                'password': 'pwd'
                }
            },
        'keyspace': 'users',
        'query': 'SELECT * from users;',
        }
    with mock.patch(
            'play_cassandra.providers.Cluster') as cluster:
        with mock.patch(
                'play_cassandra.providers.auth') as auth:
            provider.command_execute(command)
            assert cluster.assert_called_once_with(
                auth_provider=auth.PlainTextAuthProvider.return_value) is None
            assert auth.PlainTextAuthProvider.assert_called_once_with(
                username=command['connection']['auth_provider']['username'],
                password=command['connection']['auth_provider']['password']
            ) is None
            connect = cluster.return_value.__enter__.return_value.connect
            assert connect.assert_called_once_with(command['keyspace']) is None
            execute = connect.return_value.__enter__.return_value.execute
            assert execute.assert_called_once_with(command['query']) is None


def test_provider_condition_true(play_json):
    from play_cassandra import providers
    import mock
    provider = providers.CassandraProvider(play_json)
    assert provider.engine is play_json
    command = {
        'provider': 'play_cassandra',
        'type': 'execute',
        'connection': {
            'auth_provider': {
                'username': 'user',
                'password': 'pwd'
                }
            },
        'keyspace': 'users',
        'query': 'SELECT * from users;',
        'condition': '1 == 1'
        }
    with mock.patch(
            'play_cassandra.providers.Cluster') as cluster:
        with mock.patch(
                'play_cassandra.providers.auth') as auth:
            provider.command_execute(command)
            assert cluster.assert_called_once_with(
                auth_provider=auth.PlainTextAuthProvider.return_value) is None
            assert auth.PlainTextAuthProvider.assert_called_once_with(
                username=command['connection']['auth_provider']['username'],
                password=command['connection']['auth_provider']['password']
            ) is None
            connect = cluster.return_value.__enter__.return_value.connect
            assert connect.assert_called_once_with(command['keyspace']) is None
            execute = connect.return_value.__enter__.return_value.execute
            assert execute.assert_called_once_with(command['query']) is None
