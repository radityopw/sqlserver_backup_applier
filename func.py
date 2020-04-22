import config
from sqlalchemy import text
import logging

def get_config(keyword):
    logging.basicConfig(level=logging.ERROR)
    monitoring = config.monitoring_db()
    conn_monitoring = monitoring.connect()

    sql = text("SELECT keyword,value FROM config WHERE keyword = :keyword")
    result = conn_monitoring.execute(sql,keyword = keyword).fetchall()
    row = result[0]
    value = row.value
    if row.keyword in ('physical_root_dir_source','physical_root_dir_transfer', 'physical_root_dir_target') :
        value = value.replace("\\","/").lower()
    

    conn_monitoring.close()
    monitoring.dispose()

    return value


def get_ftp_file_location(id):
    logging.basicConfig(level=logging.ERROR)

    monitoring = config.monitoring_db()
    conn_monitoring = monitoring.connect()

    physical_root_dir_source = get_config('physical_root_dir_source')

    physical_root_dir_transfer = get_config('physical_root_dir_transfer')

    physical_root_dir_target = get_config('physical_root_dir_target')


    sql = text("""
            SELECT id,physical_device_name
            FROM backups
            WHERE id = :id
        """)
    files = conn_monitoring.execute(sql,id=id).fetchall()
    file = files[0].physical_device_name
    file = file.replace("\\","/").lower()

    file = file.replace(physical_root_dir_source,physical_root_dir_transfer)

    conn_monitoring.close()
    monitoring.dispose()

    return file

def get_ftp_name_file_without_protocol(id):
    logging.basicConfig(level=logging.ERROR)

    physical_root_dir_transfer = get_config('physical_root_dir_transfer')

    #splitting the value 
    physical_device_ftp = get_ftp_file_location(id)
    
    physical_device_ftp = physical_device_ftp.replace(physical_root_dir_transfer,"")

    return physical_device_ftp

def get_name_file_only(id):
    logging.basicConfig(level=logging.ERROR)

    physical_device_ftp = get_ftp_name_file_without_protocol(id)
    #print("halo " +physical_device_ftp)

    physical_device_ftp_split = physical_device_ftp.split("/")
    name_file = physical_device_ftp_split[-1]

    return name_file

def get_ftp_dir(id):
    logging.basicConfig(level=logging.ERROR)
    physical_device_ftp = get_ftp_name_file_without_protocol(id)
    physical_device_ftp_split = physical_device_ftp.split("/")
    del physical_device_ftp_split[-1]
    ftp_dir = "/"
    if len(physical_device_ftp_split) > 1 :
        ftp_dir = physical_device_ftp_split.join("/")

    return ftp_dir

def get_target_file_location(id):
    logging.basicConfig(level=logging.ERROR)
    target_file = get_config('physical_root_dir_target') + "/" + get_name_file_only(id)

    return target_file