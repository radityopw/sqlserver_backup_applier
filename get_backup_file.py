import logging
import config 
import sqlalchemy
import func
from sqlalchemy import text

logging.basicConfig(level=logging.ERROR)

logging.debug("opening db")
monitoring = config.monitoring_db()
conn_monitoring = monitoring.connect()
ftp = config.ftp_source() 


sql = text("""
            SELECT id,physical_device_name
            FROM backups
            WHERE is_downloaded IS NULL or is_downloaded = 0
        """)
files = conn_monitoring.execute(sql).fetchall()

for file in files :
    ftp_dir = func.get_ftp_dir(file.id)
    name_file = func.get_name_file_only(file.id)
    target_name_file = func.get_target_file_location(file.id)
    handle = open(target_name_file, 'wb')
    ftp.cwd(ftp_dir) 
    ftp.retrbinary('RETR %s' % name_file, handle.write)
    logging.debug("phase 4 update backups monitoring")
    sql = text("""
            UPDATE backups
            SET  is_downloaded = 1
                ,date_download = now()
            WHERE id = :id
            """)
    try : 
        trans = conn_monitoring.begin()
        conn_monitoring.execute(sql,id=file.id)
        trans.commit()
    except Exception as e :
        logging.error(e)
        trans.rollback()


logging.debug("close db")
conn_monitoring.close()
monitoring.dispose()
ftp.close()