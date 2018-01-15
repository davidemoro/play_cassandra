from cassandra.cluster import Cluster
from cassandra import auth
from pytest_play.providers import BaseProvider


class CassandraProvider(BaseProvider):
    """ Cassandra provider """

    ALLOWED_AUTH = (
        'PlainTextAuthenticator',
        'SaslAuthProvider',
    )

    def _setup_auth_provider(self, cmd):
        connection = cmd['connection']
        auth_provider_conf = connection.pop('auth_provider')
        auth_type = connection.pop('auth_type', 'PlainTextAuthProvider')
        if auth_type not in self.ALLOWED_AUTH:
            auth_provider = getattr(auth, auth_type)
        connection['auth_provider'] = auth_provider(**auth_provider_conf)

    def command_execute(self, command, **kwargs):
        cmd = command.copy()
        self._setup_auth_provider(cmd)
        connection = command['connection']
        with Cluster(**connection) as cluster:
            with cluster.connect(cmd['keyspace']) as session:
                session.execute(cmd['query'])
