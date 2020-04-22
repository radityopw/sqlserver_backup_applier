import logging
import config 
import sqlalchemy
import func
import os
from sqlalchemy import text

logging.basicConfig(level=logging.DEBUG)

logging.debug("opening db")
monitoring = config.monitoring_db()
conn_monitoring = monitoring.connect()

backup_days_retention = func.get_config('days_retention')

sql = text("""
        SELECT id,physical_device_name
        FROM backups
        WHERE is_downloaded = 1
        AND date_download >= DATE_SUB(CURDATE(), INTERVAL :days DAY) AND date_download <= CURDATE()
    """)

result = conn_monitoring.execute(sql,days = backup_days_retention)
while True :
    row = result.fetchone()
    if row == None:
        break 
    target = func.get_target_file_location(row.id)
    logging.debug("processing "+row.physical_device_name)
    logging.debug("target "+target)
    try : 
        os.remove(target)
        trans = conn_monitoring.begin()
        sql = text("UPDATE backups SET is_deleted = 1, date_deleted = now() WHERE id = :id")
        conn_monitoring.execute(sql,id=row.id)
        trans.commit()
    except Exception as e :
        trans.rollback()
        logging.error(e)


logging.debug("close db")
conn_monitoring.close()
monitoring.dispose()