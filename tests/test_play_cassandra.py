#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `play_cassandra` package."""


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
        'query': '',
        }
    with mock.patch(
            'play_cassandra.providers.Cluster') as cluster:
        with mock.patch(
                'play_cassandra.providers.auth') as auth:
            provider.command_execute(command)
            assert cluster.assert_called_once_with(auth_provider=auth.PlainTextAuthProvider.return_value) is None
            assert auth.PlainTextAuthProvider.assert_called_once_with(
                username=command['connection']['auth_provider']['username'],
                 password=command['connection']['auth_provider']['password']) is None
            connect = cluster.return_value.__enter__.return_value.connect
            assert connect.assert_called_once_with(command['keyspace']) is None
            execute = connect.return_value.__enter__.return_value.execute
            assert execute.assert_called_once_with(command['query']) is None
