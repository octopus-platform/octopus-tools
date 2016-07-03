from octopus.server.orientdb.orientdb_shell_mananger import OrientDBShellManager
from octopus.shell.octopus_shell import OctopusShellConnection

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = '2480'
DEFAULT_DATABASE_NAME = 'octopusDB'


class PythonShellInterface:

    def __init__(self):
        self._initializeDefaults()

    def _initializeDefaults(self):
        self.host = DEFAULT_HOST
        self.port = DEFAULT_PORT
        self.databaseName = DEFAULT_DATABASE_NAME

    def setHost(self, host):
        self.host = host

    def setPort(self, port):
        self.port = port

    def setDatabaseName(self, databaseName):
        self.databaseName = databaseName

    def connectToDatabase(self):
        self._createShellManagerAndConnection()
        self.shell_connection = self._getOrCreateFreeShell()

    def _getOrCreateFreeShell(self):
        shell = self._getExistingFreeShell()
        if not shell:
            shell = self._createNewShell()
        return shell

    def _getExistingFreeShell(self):
        shells = list(self.shell_manager.list())
        prefix = self._getPythonShellPrefix()

        shells = [(port, name) for (port, dbName, name, occupied) in shells
                  if name.startswith(prefix) and occupied == 'false']

        if len(shells) == 0: return None
        return self._getShellForPort(shells[0][0])

    def _getShellForPort(self, port):
        connection = OctopusShellConnection(self.host, port)
        connection.connect()
        return connection

    def _createNewShell(self):
        self.shell_manager.create(self.databaseName)

    def _getPythonShellPrefix(self):
        return ".python_" + self.databaseName

    def _createShellManagerAndConnection(self):
        self.shell_manager = OrientDBShellManager(self.host, self.port)
        self.shell_connection = OctopusShellConnection(self.host, self.port)

    def runGremlinQuery(self, query):
        pass

    """
    Create chunks from a list of ids.
    This method is useful when you want to execute many independent
    traversals on a large set of start nodes. In that case, you
    can retrieve the set of start node ids first, then use 'chunks'
    to obtain disjoint subsets that can be passed to idListToNodes.
    """
    def chunks(self, idList, chunkSize):
        for i in range(0, len(idList), chunkSize):
            yield idList[i:i+chunkSize]
