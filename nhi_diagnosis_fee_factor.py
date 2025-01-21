from csv import DictReader

input_file = 'nhi_diagnosis_fee/csv/import_data_nhi_diagnosis_fee_factor.csv'
output_file = 'nhi_diagnosis_fee/sql/output_nhi_diagnosis_fee_factor.sql'

table = 'nhi_diagnosis_fee_factor'
column_mapping = {
    'nhi_diagnosis_fee_factor_id': None,
    'seq': None,
    'insu_division_code': {'name': '健保科別', 'handler': 'default_value', 'default': 'NULL'},
    'insu_division_name': {'name': '健保科別名稱', 'handler': 'default_value', 'default': 'NULL'},
    'min_age': None,
    'max_age': None,
    'factor': None,
    'effective_date': None,
    'expiration_date': None,
    'note': {'name': '健保說明', 'handler': 'handle_line_break', 'default': 'NULL'},
    'created_at': None,
    'created_by': None,
    'max_diagnosis_count': {'name': '非山地離島最大人數', 'handler': 'number_value', 'default': 'NULL'},
    'max_diagnosis_count_mountain': {'name': '山地離島最大人數', 'handler': 'number_value', 'default': 'NULL'}
}

columns = list(column_mapping.keys())
column_value = {c: 'NULL' for c in columns}

def handle_default_value(value: str, default_value: str = 'NULL'):
    return f"'{value}'" if value else default_value

def handle_number_value(value: str, default_value: 0):
    return f"{value}" if value else default_value

def handle_line_break(value: str, default_value: str = 'NULL'):
    text = f"'{value.replace('\n', '\\n').replace('\r', '\\r').replace("'", "''")}'" if value else default_value
    # length = len(text)
    # out.write(f"/* {length} */")
    return text

def mapping_column_value(row: dict):
    for col in columns:
        source_cnf = column_mapping[col]
        if source_cnf is None:
            continue
        csv_title = source_cnf['name']
        value = row[csv_title]
        default_value = source_cnf.get('default')
        if source_cnf['handler'] == 'default_value':
            column_value[col] = handle_default_value(value, default_value)
        elif source_cnf['handler'] == 'number_value':
            column_value[col] = handle_number_value(value, default_value)
        elif source_cnf['handler'] == 'handle_line_break':
            column_value[col] = handle_line_break(value)

def write_age_segment_sql(seq: int, min_age: int, max_age: int, factor: str):
    column_value['seq'] = f"{seq}"
    column_value['min_age'] = f"{min_age}"
    column_value['max_age'] = f"{max_age}"
    column_value['factor'] = f"{factor}"
    if (seq > 1):
        out.write(",\n")
    out.write(f"({', '.join(column_value.values())})")


def write_values_sql(reader: DictReader):
    seq = 0
    for index, row in enumerate(reader):

        column_value['nhi_diagnosis_fee_factor_id'] = f"gen_random_uuid()"
        column_value['created_at'] = 'CURRENT_TIMESTAMP'
        column_value['created_by'] = '\'{"id": null, "name": "SYS", "admin": true}\''
        column_value['effective_date'] = '\'2024-01-01\''
        column_value['expiration_date'] = '\'2910-12-31\''

        mapping_column_value(row)

        write_age_segment_sql(seq+1, 0, 3, handle_number_value(row['年齡<4 歲'], 1))
        write_age_segment_sql(seq+2, 4, 6, handle_number_value(row['4≧年齡≦6'], 1))
        write_age_segment_sql(seq+3, 7, 74, handle_number_value(row['7≧年齡 ≦74'], 1))
        write_age_segment_sql(seq+4,75, 999, handle_number_value(row['年齡≧75'], 1))
        seq += 4


with open(output_file, 'w', newline='', encoding='utf-8-sig') as out:
    with open(input_file, newline='', encoding='utf-8-sig') as source:
        out.write(f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n")
        write_values_sql(DictReader(source))
        out.write("\nON CONFLICT ON CONSTRAINT nhi_diagnosis_fee_factor_unique DO UPDATE SET")
        out.write("\ninsu_division_name = EXCLUDED.insu_division_name,")
        out.write("\nmax_age = EXCLUDED.max_age,")
        out.write("\nfactor = EXCLUDED.factor,")
        out.write("\nexpiration_date = EXCLUDED.expiration_date,")
        out.write("\nnote = EXCLUDED.note,")
        out.write("\nmax_diagnosis_count = EXCLUDED.max_diagnosis_count,")
        out.write("\nmax_diagnosis_count_mountain = EXCLUDED.max_diagnosis_count_mountain,")
        out.write("\nmodified_at = CURRENT_TIMESTAMP,")
        out.write("\nmodified_by = '{\"id\": null, \"name\": \"SYS\", \"admin\": true}';")