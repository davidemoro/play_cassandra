#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `play_cassandra` package."""


def test_provider():
    from play_cassandra import providers
    import mock
    provider = providers.CassandraProvider(None)
    assert provider.engine is None
    with mock.patch('play_cassandra.providers.Cluster') as cluster:
        provider.command_execute(
            {'provider': 'play_cassandra',
             'type': 'execute',
             'connection': {'auth_provider': {'username': 'user', 'password': 'pwd'}},
             'keyspace': 'users',
             'query': 'SELECT * FROM users',
             'message': 'Hello, World!'})
