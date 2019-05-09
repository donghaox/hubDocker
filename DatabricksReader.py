from pyhive import sqlalchemy_hive
from sqlalchemy.engine import create_engine
from thrift.transport import THttpClient
import base64
import pandas as pd


class DatabricksReader(object):
    def __init__(self, token, httpPath, domain='https://eastus.azuredatabricks.net:443/'):
        self.token = token
        self.httpPath = httpPath
        self.domain = domain
        transport = THttpClient.THttpClient(self.domain + self.httpPath)
        transport.setCustomHeaders({'Authorization': 'Basic {0}'.format(
            base64.standard_b64encode('token:{0}'.format(token).encode()).decode())})
        self.engine = create_engine('hive://', connect_args={"thrift_transport": transport})

    def sql(self, query):
        return pd.read_sql_query(query, self.engine)