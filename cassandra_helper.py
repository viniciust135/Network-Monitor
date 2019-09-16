from cassandra.cluster import Cluster
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
import uuid
from datetime import datetime 

def ConnectDB( clusterIPs ):
        # cluster connection, keyspace, and protocol version
        create_keyspace( clusterIPs )
        connection.setup( clusterIPs, "network", protocol_version=3 )
        create_tables()


def create_keyspace( clusterIPs ):
        cluster = Cluster( clusterIPs )
        #This will attempt to connection to a Cassandra instance on your local machine (127.0.0.1). You can also specify a list of IP addresses for nodes in your cluster
        session = cluster.connect()
        session.execute("CREATE KEYSPACE IF NOT EXISTS network WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");	
        cluster.shutdown() 

def create_tables():
        # https://docs.datastax.com/en/developer/python-driver/3.19/api/cassandra/cqlengine/management/
        sync_table(Connection)
#        sync_table(SSH)
#        sync_table(DHCP)
#        sync_table(HTTP)
#        sync_table(DNS)
        # truncate microseconds from timestamps


def insert_connection(data):
 	Connection.create(
                 ts = datetime.fromtimestamp(data.get('ts')),
                 uid = str(data.get('uid')),
                 orig_h = str(data.get('id.orig_h')),
                 orig_p = int(data.get('id.orig_p')),
                 resp_h = str(data.get('id.resp_h')),
                 resp_p = int(data.get('id.resp_p')),
                 proto = str(data.get('proto')),
                 duration = float(0 if data.get('duration') is None else data.get('duration')),                 
                 orig_bytes = int(0 if data.get('orig_bytes') is None else data.get('orig_bytes')),
                 resp_bytes = int(0 if data.get('resp_bytes') is None else data.get('resp_bytes')), 
                 conn_state = str(data.get('conn_state')),
                 #local_orig =   False             
                 # local_resp False
                 missed_bytes = int(data.get('missed_bytes')),
                 history = str(data.get('history')),
                 orig_pkts = int(data.get('orig_pkts')),
                 orig_ip_bytes = int(data.get('orig_ip_bytes')),
                 resp_pkts = int(data.get('resp_pkts')),
                 resp_ip_bytes = int(data.get('resp_ip_bytes')),
                 orig_l2_addr = str(data.get('orig_l2_addr')),
                 resp_l2_addr = str(data.get('resp_l2_addr'))
         )
	#Connection.create(service=str(data.get('service')), duration=str(data.get('duration')), orig_bytes=str(data.get('orig_bytes')), resp_bytes=str(data.get('resp_bytes')), conn_state=str(data.get('conn_state')), local_orig=str(data.get('local_orig')), local_resp=str(data.get('local_resp')), missed_bytes=str(data.get('missed_bytes')), history=str(data.get('history')), orig_pkts=str(data.get('orig_pkts')), orig_ip_bytes=str(data.get('orig_ip_bytes')), resp_pkts=str(data.get('resp_pkts')), resp_ip_bytes=str(data.get('resp_ip_bytes')), tunnel_parents=str(data.get('tunnel_parents')), vlan=str(data.get('vlan')), inner_vlan=str(data.get('inner_vlan')), orig_l2_addr=str(data.get('orig_l2_addr')), resp_l2_addr=str(data.get('resp_l2_addr')))
        #{'conn': {'ts': 1568404324.317454, 'uid': 'CTRyjOWK8cOMG8FU7', 'id.orig_h': '200.18.42.61', 'id.orig_p': 53409, 'id.resp_h': '239.255.255.250', 'id.resp_p': 1900, 'proto': 'udp', 'duration': 3.0016438961029053, 'orig_bytes': 696, 'resp_bytes': 0, 'conn_state': 'S0', 'local_orig': False, 'local_resp': False, 'missed_bytes': 0, 'history': 'D', 'orig_pkts': 4, 'orig_ip_bytes': 808, 'resp_pkts': 0, 'resp_ip_bytes': 0, 'orig_l2_addr': '2c:59:e5:be:6a:5c', 'resp_l2_addr': '01:00:5e:7f:ff:fa'}}                       


def insert_ssh(data):
        SSH.create(
                 ts = float(data.get('ts')),
                 uid = str(data.get('uid')),
                 orig_h = str(data.get('id.orig_h')),
                 orig_p = int(data.get('id.orig_p')),
                 resp_h = str(data.get('id.resp_h')),
                 resp_p = int(data.get('id.resp_p')),
                 auth_attempts = int(data.get('auth_attempts')),
                 direction = str(data.get('direction')),
                 server = str(data.get('server')),
                 cipher_alg = str(data.get('cipher_alg')),
                 mac_alg = str(data.get('mac_alg')),
                 compression_alg=str(data.get('compression_alg')),
                 kex_alg=str(data.get('kex_alg')),
                 host_key_alg=str(data.get('host_key_alg')),
                 host_key=str(data.get('host_key'))                 
        )
        #{'ssh': {'ts': 1568581444.058824, 'uid': 'CIUI2A4puDhH5Ul6K1', 'id.orig_h': '192.168.0.20', 'id.orig_p': 49782, 'id.resp_h': '200.18.42.100', 'id.resp_p': 22, 'auth_attempts': 0, 'direction': 'OUTBOUND', 'server': 'SSH-2.0-OpenSSH_7.4p1 Debian-10+deb9u6'}}
	#SSH.create(version_1=str(data.get('version_1')), auth_success=str(data.get('auth_success')), auth_attempts=str(data.get('auth_attempts')), direction=str(data.get('direction')), client=str(data.get('client')), server=str(data.get('server')), cipher_alg=str(data.get('cipher_alg')), mac_alg=str(data.get('mac_alg')), compression_alg=str(data.get('compression_alg')), kex_alg=str(data.get('kex_alg')), host_key_alg=str(data.get('host_key_alg')), host_key=str(data.get('host_key')))

def insert_dhcp(data):
	DHCP.create(mac=str(data.get('mac')), assigned_ip=str(data.get('assigned_ip')), lease_time=str(data.get('lease_time')), trans_id1=str(data.get('trans_id1')))

def insert_http(data):
	HTTP.create(trans_depth=str(data.get('trans_depth')), method=str(data.get('method')), host=str(data.get('host')), uri=str(data.get('uri')), referrer=str(data.get('referrer')), version=str(data.get('version')), user_agent=str(data.get('user_agent')), request_body_len=str(data.get('request_body_len')), response_body_len=str(data.get('response_body_len')), status_code=str(data.get('status_code')), status_msg=str(data.get('status_msg')), info_code=str(data.get('info_code')), info_msg=str(data.get('info_msg')), tags=str(data.get('tags')), username=str(data.get('username')), password=str(data.get('password')), proxied=str(data.get('proxied')), orig_fuids=str(data.get('orig_fuids')), orig_filenames=str(data.get('orig_filenames')), orig_mime_types=str(data.get('orig_mime_types')), resp_fuids=str(data.get('resp_fuids')), resp_filenames=str(data.get('resp_filenames')), resp_mime_types=str(data.get('resp_mime_types')))

def insert_dns(data):
	DNS.create(trans_id=str(data.get('trans_id')), rtt=str(data.get('rtt')), query=str(data.get('query')), qclass=str(data.get('qclass')), qclass_name=str(data.get('qclass_name')), qtype=str(data.get('qtype')), qtype_name=str(data.get('qtype_name')), rcode=str(data.get('rcode')), rcode_name=str(data.get('rcode_name')), aa=str(data.get('aa')), tc=str(data.get('tc')), rd=str(data.get('rd')), ra=str(data.get('ra')), z=str(data.get('z')), answers=str(data.get('answers')), ttls=str(data.get('ttls')), rejected=str(data.get('rejected')), addl=str(data.get('addl')), auth=str(data.get('auth')))

#{'conn': {'ts': 1568404324.317454, 'uid': 'CTRyjOWK8cOMG8FU7', 'id.orig_h': '200.18.42.61', 'id.orig_p': 53409, 'id.resp_h': '239.255.255.250', 'id.resp_p': 1900, 'proto': 'udp', 'duration': 3.0016438961029053, 'orig_bytes': 696, 'resp_bytes': 0, 'conn_state': 'S0', 'local_orig': False, 'local_resp': False, 'missed_bytes': 0, 'history': 'D', 'orig_pkts': 4, 'orig_ip_bytes': 808, 'resp_pkts': 0, 'resp_ip_bytes': 0, 'orig_l2_addr': '2c:59:e5:be:6a:5c', 'resp_l2_addr': '01:00:5e:7f:ff:fa'}}                       
class Connection(Model):
        #columns.DateTime.truncate_microseconds = True                
        id = columns.UUID(primary_key=True, default=uuid.uuid4)
        ts = columns.DateTime(required=True)
        uid = columns.Text(required=True)
        orig_h = columns.Inet(required=True)
        orig_p = columns.Integer(required=True)
        resp_h = columns.Inet(required=True)
        resp_p = columns.Integer(required=True)
        proto = columns.Text(required=True)
        duration = columns.Float(required=False)
        orig_bytes = columns.Integer(required=False)
        resp_bytes = columns.Integer(required=False)
        conn_state = columns.Text(required=False)
        #local_orig =   False             
        # local_resp False
        missed_bytes = columns.Integer(required=False)
        history = columns.Text(required=False)
        orig_pkts = columns.Integer(required=False)
        orig_ip_bytes = columns.Integer(required=False)
        resp_pkts = columns.Integer(required=False)
        resp_ip_bytes = columns.Integer(required=False)
        orig_l2_addr = columns.Text(required=False)
        resp_l2_addr = columns.Text(required=False)
        

class SSH(Model):
        id = columns.UUID(primary_key=True, default=uuid.uuid4)
        ts = columns.Float(required=False)
        uid = columns.Text(required=False)
        orig_h = columns.Text(required=False)
        orig_p = columns.Integer(required=False)
        resp_h = columns.Text(required=False)
        resp_p = columns.Integer(required=False)
        auth_attempts = columns.Integer(required=False)
        direction = columns.Text(required=False)
        server = columns.Text(required=False)
        cipher_alg = columns.Text(required=False)
        mac_alg = columns.Text(required=False)
        compression_alg = columns.Text(required=False)
        kex_alg = columns.Text(required=False)
        host_key_alg = columns.Text(required=False)
        host_key = columns.Text(required=False)

class DHCP(Model):
        id = columns.UUID(primary_key=True, default=uuid.uuid4)
        mac  = columns.Text(required=False)
        assigned_ip = columns.Text(required=False)
        lease_time = columns.Text(required=False)
        trans_id1 = columns.Text(required=False)

class HTTP(Model):
        id = columns.UUID(primary_key=True, default=uuid.uuid4)
        trans_depth  = columns.Text(required=False)
        method = columns.Text(required=False)
        host = columns.Text(required=False)
        uri = columns.Text(required=False)
        referrer = columns.Text(required=False)
        version = columns.Text(required=False)
        user_agent = columns.Text(required=False)
        request_body_len = columns.Text(required=False)
        response_body_len = columns.Text(required=False)
        status_code = columns.Text(required=False)
        status_msg = columns.Text(required=False)
        info_code = columns.Text(required=False)
        info_msg = columns.Text(required=False)
        tags = columns.Text(required=False)
        username = columns.Text(required=False)
        password = columns.Text(required=False)
        proxied = columns.Text(required=False)
        orig_fuids = columns.Text(required=False)
        orig_filenames = columns.Text(required=False)
        orig_mime_types = columns.Text(required=False)
        resp_fuids = columns.Text(required=False)
        resp_filenames = columns.Text(required=False)
        resp_mime_types = columns.Text(required=False)

class DNS(Model):
        id = columns.UUID(primary_key=True, default=uuid.uuid4)
        trans_id  = columns.Text(required=False)
        rtt = columns.Text(required=False)
        query = columns.Text(required=False)
        qclass = columns.Text(required=False)
        qclass_name = columns.Text(required=False)
        qtype = columns.Text(required=False)
        qtype_name = columns.Text(required=False)
        rcode = columns.Text(required=False)
        rcode_name = columns.Text(required=False)
        aa = columns.Text(required=False)
        tc = columns.Text(required=False)
        rd = columns.Text(required=False)
        ra = columns.Text(required=False)
        z = columns.Text(required=False)
        answers = columns.Text(required=False)
        ttls = columns.Text(required=False)
        rejected = columns.Text(required=False)
        addl = columns.Text(required=False)
        auth = columns.Text(required=False)



#https://docs.datastax.com/en/developer/python-driver/3.19/getting_started/

'''class Connect_db:
        def __init__(self):
                cluster = Cluster(['172.17.0.1', '172.17.0.2']) #This will attempt to connection to a Cassandra instance on your local machine (127.0.0.1). You can also specify a list of IP addresses for nodes in your cluster
                self.session = cluster.connect()
                print(self.session)
                self.session.execute("CREATE KEYSPACE IF NOT EXISTS packets WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 }");
                #self.session.execute("CREATE COLUMNFAMILY IF NOT EXISTS conn (id )");
                #self.session.execute("CREATE KEYSPACE IF NOT EXISTS packets WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 }");
                #self.session.execute("CREATE KEYSPACE IF NOT EXISTS packets WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 }");
                #self.session.execute("CREATE KEYSPACE IF NOT EXISTS packets WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 }");
                #self.session.execute("CREATE KEYSPACE IF NOT EXISTS packets WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 }");
                self.session.set_keyspace('packets')

        def close(self): #Fecha conexão
                self.session.close()
        
        #def put_data_conn(self, data): #Insere dados'''

'''
rows = session.execute('SELECT name, age, email FROM users')
for row in rows:
    print row.name, row.age, row.email

session.execute(
    """
    INSERT INTO users (name, credits, user_id, username)
    VALUES (%(name)s, %(credits)s, %(user_id)s, %(name)s)
    """,
    {'name': "John O'Reilly", 'credits': 42, 'user_id': uuid.uuid1()}
)'''
