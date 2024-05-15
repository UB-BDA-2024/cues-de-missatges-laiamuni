from cassandra.cluster import Cluster

class CassandraClient:
    def __init__(self, hosts):
        self.cluster = Cluster(hosts,protocol_version=4)
        self.session = self.cluster.connect()

        self.session.execute("CREATE KEYSPACE IF NOT EXISTS sensor WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1};")
        self.session.execute("USE sensor;")
        self.session.execute("CREATE TABLE IF NOT EXISTS temp_values(sensor_id INT, temp FLOAT, PRIMARY KEY(sensor_id, temp));")
        self.session.execute("CREATE TABLE IF NOT EXISTS quantity(sensor_id INT, sensor_type text, PRIMARY KEY(sensor_type, sensor_id));")
        self.session.execute("CREATE TABLE IF NOT EXISTS low_bat(sensor_id INT, battery FLOAT, PRIMARY KEY(battery, sensor_id));")

    def get_session(self):
        return self.session

    def close(self):
        self.cluster.shutdown()

    def execute(self, query):
        return self.get_session().execute(query)