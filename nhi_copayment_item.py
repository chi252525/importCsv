from csv import DictReader
from uuid import uuid4

input_file = 'nhi_copayment_item/csv/import_data_nhi_copayment_item.csv'
output_file = 'nhi_copayment_item/sql/output.sql'

table = 'nhi_copayment_item'
column_mapping = {
    'nhi_copayment_item_id': None,
    'seq': None,
    'copayment_code': {'name': 'copayment_code', 'handler': 'default_value', 'default': 'NULL'},
    'copayment_name': {'name': 'copayment_name', 'handler': 'default_value', 'default': 'NULL'},
    'description': {'name': 'description', 'handler': 'default_value', 'default': 'NULL'},
    'copayment_price': {'name': 'copayment_price', 'handler': 'number_value', 'default': '0'},
    'medical_grade': {'name': 'medical_grade', 'handler': 'default_value', 'default': 'NULL'},
    'is_stop': {'name': 'is_stop', 'handler': 'yn_to_bool'},
    'stop_datetime': None,
    'created_at': None,
    'created_by': None
}

columns = list(column_mapping.keys())
column_value = {c: 'NULL' for c in columns}

def handle_default_value(value: str, default_value: str = 'NULL'):
    return f"'{value}'" if value else default_value

def handle_number_value(value: str, default_value: 0):
    return f"{value}" if value else default_value

def handle_yn_to_bool(value: str):
    return 'TRUE' if value == 'Y' else 'FALSE'

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
        elif source_cnf['handler'] == 'strip':
            column_value[col] = handle_default_value(value.strip(), default_value)
        elif source_cnf['handler'] == 'escape_single_quote':
            column_value[col] = handle_default_value(value.strip().replace("'", "''"), default_value)
        elif source_cnf['handler'] == 'yn_to_bool':
            column_value[col] = handle_yn_to_bool(value)


def write_values_sql(reader: DictReader):
    seq = 1
    for index, row in enumerate(reader):
        if index != 0:
            out.write(",\n")
        uuid = '10000000-0000-0000-0000-' + str(seq).zfill(12)

        # column_value['system_code_id'] = f"'{uuid4()}'"
        column_value['nhi_copayment_item_id'] = f"'{uuid}'"
        column_value['seq'] = f"{seq}"
        column_value['stop_datetime'] = 'NULL'
        column_value['created_at'] = 'CURRENT_TIMESTAMP'
        column_value['created_by'] = '\'{"id": null, "name": "SYS", "admin": true}\''

        mapping_column_value(row)

        out.write(f"({', '.join(column_value.values())})")

        seq += 1


with open(output_file, 'w', newline='', encoding='utf-8-sig') as out:
    with open(input_file, newline='', encoding='utf-8-sig') as source:
        out.write(f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n")
        write_values_sql(DictReader(source))
        out.write("\nON CONFLICT ON CONSTRAINT nhi_copayment_item_pk DO UPDATE SET")
        out.write("\nseq = EXCLUDED.seq,")
        out.write("\ncopayment_code = EXCLUDED.copayment_code,")
        out.write("\ncopayment_name = EXCLUDED.copayment_name,")
        out.write("\ndescription = EXCLUDED.description,")
        out.write("\ncopayment_price = EXCLUDED.copayment_price,")
        out.write("\nmedical_grade = EXCLUDED.medical_grade,")
        out.write("\nis_stop = EXCLUDED.is_stop,")
        out.write("\nstop_datetime = EXCLUDED.stop_datetime,")
        out.write("\nmodified_at = CURRENT_TIMESTAMP,")
        out.write("\nmodified_by = '{\"id\": null, \"name\": \"SYS\", \"admin\": true}';")
