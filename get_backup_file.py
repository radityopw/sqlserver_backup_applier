import logging
import config 
import sqlalchemy
from sqlalchemy import text

#logging.basicConfig(level=logging.DEBUG)

logging.debug("opening db")
monitoring = config.monitoring_db()
conn_monitoring = monitoring.connect()
ftp = config.ftp_source() 

sql = text("""
            SELECT keyword,value FROM config WHERE keyword IN ('physical_root_dir_source','physical_root_dir_transfer','physical_root_dir_target')
        """)
result = conn_monitoring.execute(sql).fetchall()
for row in result :
    if row.keyword == 'physical_root_dir_source' :
        physical_root_dir_source = row.value
        physical_root_dir_source = physical_root_dir_source.replace("\\","/")
        physical_root_dir_source.lower()
    if row.keyword == 'physical_root_dir_transfer' :
        physical_root_dir_transfer = row.value
        physical_root_dir_transfer = physical_root_dir_transfer.replace("\\","/")
        physical_root_dir_transfer.lower()
    if row.keyword == 'physical_root_dir_target' :
        physical_root_dir_target = row.value
        physical_root_dir_target = physical_root_dir_target.replace("\\","/")
        physical_root_dir_target.lower()

logging.debug("physical_root_dir_source "+ physical_root_dir_source)
logging.debug("physical_root_dir_transfer " +physical_root_dir_transfer)
logging.debug("physical_root_dir_target " +physical_root_dir_target)

sql = text("""
            SELECT id,physical_device_name
            FROM backups
            WHERE is_downloaded IS NULL or is_downloaded = 0
        """)
files = conn_monitoring.execute(sql).fetchall()

for file in files :
    physical_device_name = file.physical_device_name
    physical_device_name = physical_device_name.replace("\\","/").lower()
    physical_device_ftp = physical_device_name.replace(physical_root_dir_source,physical_root_dir_transfer)
    logging.debug("Processing "+physical_device_name)
    logging.debug("phase 1 "+physical_device_ftp)

    #splitting the value 
    physical_device_ftp = physical_device_ftp.replace(physical_root_dir_transfer,"")
    physical_device_ftp_split = physical_device_ftp.split("/")
    logging.debug("phase 2 (list) "+str(physical_device_ftp_split))
    name_file = physical_device_ftp_split[-1]
    logging.debug("phase 2 (name) "+str(name_file))
    del physical_device_ftp_split[-1]
    logging.debug("phase 2 (list) "+str(physical_device_ftp_split))
    logging.debug("phase 2 len (list) "+str(len(physical_device_ftp_split)))
    target_name_file = physical_root_dir_target + "/" + name_file
    logging.debug("phase 2 (target name) "+target_name_file)
    ftp_dir = "/"
    if len(physical_device_ftp_split) > 1 :
        ftp_dir = physical_device_ftp_split.join("/")
    logging.debug("phase 2 (ftp_dir) "+ftp_dir)
    logging.debug("phase 3 copy files ")
    handle = open(target_name_file, 'wb')
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