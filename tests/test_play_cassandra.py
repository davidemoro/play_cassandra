#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `play_cassandra` package."""


def test_provider():
    from play_cassandra import providers
    provider = providers.CassandraProvider(None)
    assert provider.engine is None
    provider.command_execute(
        {'provider': 'play_cassandra',
         'type': 'execute',
         'message': 'Hello, World!'})
