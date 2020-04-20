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


sql = text("SELECT value as val FROM config WHERE keyword = 'days_retention'")
result = conn_monitoring.execute(sql).fetchall()
days_retention = int(result[0].val)


sql = text("""
        SELECT 
        msdb.dbo.backupset.database_name, 
        msdb.dbo.backupset.backup_finish_date, 
        CASE msdb..backupset.type 
        WHEN 'D' THEN 'Database' 
        WHEN 'L' THEN 'Log' 
        WHEN 'I' THEN 'Diff'
        END AS backup_type,
        msdb.dbo.backupset.backup_size,  
        msdb.dbo.backupmediafamily.physical_device_name 
        FROM msdb.dbo.backupmediafamily 
        INNER JOIN msdb.dbo.backupset ON msdb.dbo.backupmediafamily.media_set_id = msdb.dbo.backupset.media_set_id 
        WHERE (CONVERT(datetime, msdb.dbo.backupset.backup_start_date, 102) >= GETDATE() - :dr) 
        ORDER BY 
        msdb.dbo.backupset.database_name, 
        msdb.dbo.backupset.backup_finish_date 
      """)

backups = conn_sqlserver.execute(sql,dr = days_retention).fetchall()
for backup in backups:
    logging.debug("processing "+backup.physical_device_name)
    #print(backup)
    sql = text("""
                INSERT INTO backups(id,database_name,backup_finish_date,backup_type,backup_size,physical_device_name)
                VALUES(uuid(),:database_name,:backup_finish_date,:backup_type,:backup_size,:physical_device_name)
                """)
    try :
        trans = conn_monitoring.begin()
        conn_monitoring.execute(sql,database_name=backup.database_name,backup_finish_date=backup.backup_finish_date,backup_type=backup.backup_type,backup_size=backup.backup_size,physical_device_name=backup.physical_device_name)
        trans.commit()
    except sqlalchemy.exc.IntegrityError as ie :
        logging.debug("duplicate, skip record")
        trans.rollback()
    except Exception as e :
        logging.error(e)
        trans.rollback()


logging.debug("close db")
conn_sqlserver.close()
conn_monitoring.close()
sqlserver.dispose()
monitoring.dispose()