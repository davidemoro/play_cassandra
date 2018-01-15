from pytest_play.providers import BaseProvider


class CassandraProvider(BaseProvider):
    """ Cassandra provider """

    def command_execute(self, command, **kwargs):
        print(command)
