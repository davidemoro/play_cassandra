#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `play_cassandra` package."""


def test_provider():
    from play_cassandra import providers
    print_provider = providers.NewProvider(None)
    assert print_provider.engine is None
    print_provider.command_print(
        {'provider': 'play_cassandra',
         'type': 'print',
         'message': 'Hello, World!'})
