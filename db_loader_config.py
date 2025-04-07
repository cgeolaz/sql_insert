DB_CONFIG = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': 'SQL_SERVER_FQDN,port',
    'database': 'name_of_db',
    'trusted_connection': 'yes' #this is windows authentication
}

#define the columns of the DB
COLUMNS = [
    ('column1', 'datatime2'),
    ('column2', 'datetime2'),
    ('column3', 'int'),
    ('column4', nvarchar(10))
   ]
   
ROOT_DIR = "./path_to_files_to_be_inserted_into_db)
TABLE_NAME = 'table_to_insert_file_data'
BATCH_SIZE = 1000 #number of rows to insert to DB at once
LOG_FILE = './path_to_logfiles'