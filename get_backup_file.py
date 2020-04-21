import logging
import config 
import sqlalchemy
from sqlalchemy import text

#logging.basicConfig(level=logging.DEBUG)

logging.debug("opening db")
sqlserver = config.sqlserver_source()
monitoring = config.monitoring_db()

conn_sqlserver = sqlserver.connect()
conn_monitoring = monitoring.connect()




logging.debug("close db")
conn_sqlserver.close()
conn_monitoring.close()
sqlserver.dispose()
monitoring.dispose()