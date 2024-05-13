import json

def window_by_datetime(data: pd.DataFrame, window: str) -> Dict[str, pd.DataFrame]:
    #print("head \n", data) 
    data['date_time'] = pd.to_datetime(data['date_time'])
    #print("data wind\n", data)
    windows = data.set_index('date_time').groupby(pd.Grouper(freq=window))
    #print("windows\n", windows)
    return {str(window): window_data.reset_index(drop=True) for window, window_data in windows}

def process_to_RO(data: Dict[str, pd.DataFrame]) -> List[RO]:
    ro_list = []
    for window, window_data in data.items():
        #print("data \n", window)
        #print("Wind daata", window_data)
        for index, row in window_data.iterrows():
            #print("index row", index)
            order_id = row['order_id']
            date_time = datetime.strptime(window, '%Y-%m-%d %H:%M:%S')
            status = row['status']
            cost = row['cost']
            technician = row['technician']
            repair_parts = row['parts']
            ro_list.append(RO(order_id, date_time, status, cost, technician, repair_parts))
    return ro_list

def create_database(ro_data: List[RO], db_name: str):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS repair_orders (
                    order_id TEXT,
                    date_time TIMESTAMP,
                    status TEXT,
                    cost REAL,
                    technician TEXT,
                    repair_parts TEXT
                )''')
    for ro in ro_data:
        print("ro", ro)
        #c.execute("INSERT INTO repair_orders VALUES (?, ?, ?, ?, ?, ?)", ro)
        # Serialize repair_parts to JSON
        repair_parts_json = json.dumps(ro.repair_parts)
        # Insert data into the database
        c.execute("INSERT INTO repair_orders VALUES (?, ?, ?, ?, ?, ?)",
                  (ro.order_id, ro.date_time, ro.status, ro.cost, ro.technician, repair_parts_json))
    conn.commit()
    conn.close()

# Define the directory where the XML files are located
xml_dir = 'https://raw.githubusercontent.com/dtdataplatform/data-challenges/main/data-engineer/data'

# Read XML files
xml_files = read_files_from_dir(xml_dir)
#print("empty df", df)
# Parse XML content into a DataFrame
df = parse_xml(xml_files)
#print("filled df", df)

# Window the DataFrame by date_time
windowed_data = window_by_datetime(df, '1D')

# Process windowed data into structured RO format
ro_data = process_to_RO(windowed_data)

# Write the output to a SQLite database
db_name = 'repair_orders.db'
create_database(ro_data, db_name)
