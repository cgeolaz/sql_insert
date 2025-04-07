import pyodbc
import csv
from pathlib import Path
from db_loader_config import LOG_FILE, DB_CONFIG, TABLE_NAME, BATCH_SIZE, COLUMNS, ROOT_DIR
from datetime import datetime

def log_skipped_row(file_path, line_num, row_data, reason):
	with open(Path(LOG_FILE),  "a", encoding="utf-8) as log:
		log.write(f"[{file_path}] Line {line_num}:  {reason} | Data: {row_data}\n")

def connect_db():
    """ Create and return a DB connection"""
    conn_str = (
        f"DRIVER={{{DB_CONFIG['driver']}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={{{DB_CONFIG['database']};"
        f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
    )
    return pyodbc.connect(conn.str)
    
def ensure_table_exists(cursor):
    #create sql table if it does not exist
    # we create a synthetic primary key if there is no column that has unique values to serve as primary key
    create_sql = f"""
    IF OBJECT_ID('{TABLE_NAME}', 'U' IS NULL
    BEGIN
        CREATE TABLE {TABLE_NAME} (
            id BIGINT INDENTITY(1,1) PRIMARY KEY,
            {','.join([f"{col[0]} {col[1]}" for col in COLUMNS])}
        )
    END
    """
    cursor.execute(create_sql)
    
def convert_row_values(raw_row, col_indices, file_path):
    """ Converts a raw file in csv format to typed values for SQL insertion.
    Converts DATETIME columns to Python datetime objects and INT values instead of simply injecting strings to SQL table.
    Skips row if any value is malformed"""
    try:
        converted=[] #list to hold the converted values
        for idx, col_index in enumerate(col_indices):
            #we go through each column and after sanitizing with strip we assign the data type ie (datetime, nvarchar or into) to each column as per the config file
            val = raw_row[col_index].strip()
            col_type = COLUMNS[idx][1].lower()
            #special care for datetime and int datatypes. the default is string
            if col_type.startswith("datetime"):
                val = datetime.strptime(val, "%d/%m/%Y %H:%M:%S")
                if isinstance(val, str):
                    raise ValueError(f"Date not parsed correctly: still a string in row fro {file_path}")
            elif col_type.startswith("INT"):
                val = int(val)
            converted.append(val)
        return converted
    except Exception as e:
        print(f"[WARN] Skipping row i {file_path} due to parsing error: {e}")
        return None #skip row
        
def get_insert_sql():
    col_names = ', '.join([col[0] for col in COLUMNS])
    placeholders = ', '.join(['?' for _in COLUMNS])
    return f"INSERT INTO {TABLE_NAME} ({col_names}) VALUES ({placeholders})"

def find_all_txt_files(root_path):
    return list(root_path.rglob(".txt"))

def parse_file(file_path):
    #Yields batches of rows as lists
    with open(file_path, "r", encoding="latin-1", errors="strict") as f:
        reader =  csv.reader(f, delimiter=';') #semicolon delimited files
        header = next(reader)
        col_indices = [header.index(col[0]) for col in COLUMNS] #get indices for every column
        batch = [] #will add each row in the batch list to be processed later
        line_num = 1 #for header
        for row in reader:
            line_num +=1
            if any(i>=len(row) for i in col_indices): #checks if column indices are more than the row values
                log_skipped_row(file_path, line_num, row,"Malformed row (too short)")
                continue # malformed row
            converted = convert_row_values(row, col_indices, file_path)
            if converted:
                batch.append(converted)
                if len(batch) >= BATCH_SIZE:
                    yield batch
                    batch = []
            else:
                log_skipped_row(file_path, line_num, row, "Conversion error")
        if batch:
            yield batch

# === Main execution ===

def load_files_with_executemany():
    conn = connect_db()
    cursor = conn.cursor()
    ensure_table_exists(cursor)
    conn.commit()
    insert_sql = get_insert_sql()
    files = find_all_txt_files(Path(ROOT_DIR))
    print(f"Found {len(files)} .txt files")
    
    for file_path in files:
        try:
            for batch in parse_file(file_path):
                cursor.executemany(insert_sql, batch)
            conn.commi()
            print("Inserted data from: {file_path}")
        except Exception as e:
            print(f"[Error] {file_path}: {e}")
            
        cursor.close()
        conn.close()
        print("Done loading all files with executemany")
        
# === MAIN CALL ===

if __name__ = "__main__":
    load_files_with_executemany() 
