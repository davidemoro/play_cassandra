==============
play cassandra
==============


.. image:: https://img.shields.io/pypi/v/play_cassandra.svg
        :target: https://pypi.python.org/pypi/play_cassandra

.. image:: https://img.shields.io/travis/davidemoro/play_cassandra.svg
        :target: https://travis-ci.org/davidemoro/play_cassandra

.. image:: https://readthedocs.org/projects/play-cassandra/badge/?version=latest
        :target: https://play-cassandra.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://codecov.io/gh/davidemoro/play_cassandra/branch/develop/graph/badge.svg
        :target: https://codecov.io/gh/davidemoro/play_cassandra


pytest-play support for Cassandra expressions and assertions

More info and examples on:

* pytest-play_, documentation
* cookiecutter-qa_, see ``pytest-play`` in action with a working example if you want to start hacking


Features
--------

This project defines a new pytest-play_ command:

::

    {
     'provider': 'play_cassandra'
     'type': 'execute',
     'connection': {
       'contact_points': ['10.1.1.3', '10.1.1.4', '10.1.1.5'],
       'port': '9042',
       'auth_provider': {'username': '$username', 'password': '$password'}
     },
     'keyspace': 'users',
     'query': 'SELECT name, age, email FROM users WHERE user_id=15',
     'variable': 'user1_age',
     'variable_expression': 'results[0].age',
     'assertion': 'results[0].name == "User 1"'
    }

Twitter
-------

``pytest-play`` tweets happens here:

* `@davidemoro`_

Credits
-------

This package was created with Cookiecutter_ and the cookiecutter-play-plugin_ (based on `audreyr/cookiecutter-pypackage`_ project template).

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
.. _`cookiecutter-play-plugin`: https://github.com/davidemoro/cookiecutter-play-plugin
.. _pytest-play: https://github.com/pytest-dev/pytest-play
.. _cookiecutter-qa: https://github.com/davidemoro/cookiecutter-qa
.. _`@davidemoro`: https://twitter.com/davidemoro
