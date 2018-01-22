import logging
from copy import deepcopy
from cassandra.cluster import Cluster
from cassandra import auth
from pytest_play.providers import BaseProvider


class CassandraProvider(BaseProvider):
    """ Cassandra provider """

    ALLOWED_AUTH = (
        'PlainTextAuthenticator',
        'SaslAuthProvider',
    )

    def __init__(self, engine):
        super(CassandraProvider, self).__init__(engine)
        self.logger = logging.getLogger()

    def _setup_auth_provider(self, cmd):
        connection = cmd['connection']
        auth_provider_conf = connection.pop('auth_provider')
        auth_type = connection.pop('auth_type', 'PlainTextAuthProvider')
        if auth_type not in self.ALLOWED_AUTH:
            auth_provider = getattr(auth, auth_type)
        connection['auth_provider'] = auth_provider(**auth_provider_conf)

    def _get_session(self, command, connection, keyspace):
        """ Get cached session """
        self.engine.register_teardown_callback(self._teardown)

        if not hasattr(self.engine, 'play_cassandra'):
            self.engine.play_cassandra = {}
        play_cassandra = self.engine.play_cassandra
        connection_key = repr(command['connection'])
        if connection_key not in play_cassandra:
            play_cassandra[connection_key] = dict(
                cluster=Cluster(**connection),
                sessions={},
            )
        if keyspace not in play_cassandra[connection_key]['sessions']:
            cluster = play_cassandra[connection_key]['cluster']
            play_cassandra[connection_key]['sessions'][keyspace] = \
                cluster.connect(keyspace)
        return play_cassandra[connection_key]['sessions'][keyspace]

    def _teardown(self):
        """ Shutdown all cluster and session open connections """
        if hasattr(self.engine, 'play_cassandra'):
            for cluster_dict in self.engine.play_cassandra.values():
                for session in cluster_dict['sessions'].values():
                    try:
                        session.shutdown()
                    except Exception:
                        pass
                try:
                    cluster_dict['cluster'].shutdown()
                except Exception:
                    pass

    def command_execute(self, command, **kwargs):
        cmd = deepcopy(command)
        self._setup_auth_provider(cmd)
        connection = cmd['connection']
        session = self._get_session(command, connection, cmd['keyspace'])
        results = session.execute(cmd['query'])
        try:
            self._make_variable(command, results=results)
            self._make_assertion(command, results=results)
        except Exception as e:
            self.logger.exception(
                'Exception for command %r',
                command,
                e)
            raise e

    def _make_assertion(self, command, **kwargs):
        """ Make an assertion based on python
            expression against kwargs
        """
        assertion = command.get('assertion', None)
        if assertion:
            self.engine.execute_command(
                {'provider': 'python',
                 'type': 'assert',
                 'expression': assertion
                 },
                **kwargs,
            )

    def _make_variable(self, command, **kwargs):
        """ Make a variable based on python
            expression against kwargs
        """
        expression = command.get('variable_expression', None)
        if expression:
            self.engine.execute_command(
                {'provider': 'python',
                 'type': 'store_variable',
                 'name': command['variable'],
                 'expression': expression
                 },
                **kwargs,
            )
