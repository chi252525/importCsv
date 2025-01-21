from csv import DictReader
from uuid import uuid4

input_file = 'nhi_diagnosis_fee/csv/import_data_nhi_service_pay_item.csv'
output_file = 'nhi_diagnosis_fee/sql/output_nhi_service_pay_item.sql'

map_input_file = 'nhi_diagnosis_fee/csv/import_data_nhi_diagnosis_fee_map.csv'
map_output_file = 'nhi_diagnosis_fee/sql/output_nhi_diagnosis_fee_map.sql'

table = 'nhi_service_pay_item'
column_mapping = {
    'nhi_service_pay_item_id': None,
    'seq': None,
    'insu_order_code': {'name': '診療項目代碼', 'handler': 'default_value', 'default': 'NULL'},
    'ch_name': {'name': '診療項目中文', 'handler': 'default_value', 'default': 'NULL'},
    'en_name': {'name': '診療項目英文', 'handler': 'default_value', 'default': 'NULL'},
    'pay_points': {'name': '支付點數', 'handler': 'number_value', 'default': '0'},
    'effective_date': {'name': '生效起日', 'handler': 'handle_roc_date', 'default': 'NULL'},
    'expiration_date': {'name': '生效訖日', 'handler': 'handle_roc_date', 'default': 'NULL'},
    'order_type_code': {'name': '品項類別', 'handler': 'handle_service_pay_type', 'default': 'NULL'},
    'note': {'name': '給付說明', 'handler': 'handle_line_break', 'default': 'NULL'},
    'indication': {'name': '適應症', 'handler': 'handle_line_break', 'default': 'NULL'},
    'created_at': None,
    'created_by': None
}

map_table = 'nhi_diagnosis_fee_map'
map_column_mapping = {
    'nhi_diagnosis_fee_map_id': None,
    'seq': None,
    'insu_order_code': {'name': '健保醫令代碼', 'handler': 'default_value', 'default': 'NULL'},
    'min_diagnosis_count': {'name': '人次(起)', 'handler': 'number_value', 'default': '0'},
    'max_diagnosis_count': {'name': '人次(訖)', 'handler': 'number_value', 'default': '0'},
    'is_prescription': {'name': '交付', 'handler': 'circle_to_bool'},
    'is_chronic': {'name': '慢籤', 'handler': 'circle_to_bool'},
    'is_chronic_continuous': {'name': '慢連籤', 'handler': 'circle_to_bool'},
    'is_mountain_area': {'name': '山地', 'handler': 'circle_to_bool'},
    'is_psychiatry': {'name': '精神科', 'handler': 'circle_to_bool'},
    'effective_date': None,
    'expiration_date': None,
    'note': None,
    'created_at': None,
    'created_by': None
}

service_pay_type_mapping = {
    '診察費':'01',
    '藥事服務費':'09'
}

columns = list(column_mapping.keys())
column_value = {c: 'NULL' for c in columns}

map_columns = list(map_column_mapping.keys())
map_column_value = {c: 'NULL' for c in map_columns}

mapping_effective_date = {}

def handle_default_value(value: str, default_value: str = 'NULL'):
    return f"'{value}'" if value else default_value

def handle_number_value(value: str, default_value: 0):
    return f"{value}" if value else default_value

def handle_yn_to_bool(value: str):
    return 'TRUE' if value == 'Y' else 'FALSE'

def handle_roc_date(roc_date: str):
    if roc_date:
        year, month, day = roc_date.split('.')
        year = str(int(year) + 1911)
        return f"'{year}-{month}-{day}'"
    return 'NULL'

def handle_service_pay_type(value: str):
    return f"'{service_pay_type_mapping[value]}'"

def handle_line_break(value: str, default_value: str = 'NULL'):
    text = f"'{value.replace('\n', '\\n').replace('\r', '\\r').replace("'", "''")}'" if value else default_value
    # length = len(text)
    # out.write(f"/* {length} */")
    return text

def handle_circle_to_bool(value: str):
    return 'TRUE' if value == '◎' else 'FALSE'

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
        elif source_cnf['handler'] == 'handle_roc_date':
            column_value[col] = handle_roc_date(value)
        elif source_cnf['handler'] == 'handle_service_pay_type':
            column_value[col] = handle_service_pay_type(value)
        elif source_cnf['handler'] == 'handle_line_break':
            column_value[col] = handle_line_break(value)

def mapping_map_column_value(row: dict):
    for col in map_columns:
        source_cnf = map_column_mapping[col]
        if source_cnf is None:
            continue
        csv_title = source_cnf['name']
        value = row[csv_title]
        default_value = source_cnf.get('default')
        if source_cnf['handler'] == 'default_value':
            map_column_value[col] = handle_default_value(value, default_value)
        elif source_cnf['handler'] == 'number_value':
            map_column_value[col] = handle_number_value(value, default_value)
        elif source_cnf['handler'] == 'handle_roc_date':
            map_column_value[col] = handle_roc_date(value)
        elif source_cnf['handler'] == 'handle_service_pay_type':
            map_column_value[col] = handle_service_pay_type(value)
        elif source_cnf['handler'] == 'handle_line_break':
            map_column_value[col] = handle_line_break(value)
        elif source_cnf['handler'] == 'circle_to_bool':
            map_column_value[col] = handle_circle_to_bool(value)

def write_values_sql(reader: DictReader):
    seq = 1
    for index, row in enumerate(reader):
        if service_pay_type_mapping.get(row['品項類別']) is None:
           continue
        if row['品項類別'] == '診察費':
            mapping_effective_date[row['診療項目代碼']] = row['生效起日'], row['生效訖日']
        if index != 0:
            out.write(",\n")
        # uuid = '10000000-0000-0000-0000-' + str(seq).zfill(12)

        # column_value['system_code_id'] = f"'{uuid4()}'"
        column_value['nhi_service_pay_item_id'] = f"gen_random_uuid()"
        column_value['seq'] = f"{seq}"
        column_value['created_at'] = 'CURRENT_TIMESTAMP'
        column_value['created_by'] = '\'{"id": null, "name": "SYS", "admin": true}\''

        mapping_column_value(row)

        out.write(f"({', '.join(column_value.values())})")

        seq += 1

def write_map_values_sql(reader: DictReader):
    seq = 1
    for index, row in enumerate(reader):
        if index != 0:
            out.write(",\n")
        # uuid = '10000000-0000-0000-0000-' + str(seq).zfill(12)

        # column_value['system_code_id'] = f"'{uuid4()}'"
        map_column_value['nhi_diagnosis_fee_map_id'] = f"gen_random_uuid()"
        map_column_value['seq'] = f"{seq}"
        map_column_value['effective_date'] = f"{handle_roc_date(mapping_effective_date[row['健保醫令代碼']][0])}"
        map_column_value['expiration_date'] = f"{handle_roc_date(mapping_effective_date[row['健保醫令代碼']][1])}"
        map_column_value['created_at'] = 'CURRENT_TIMESTAMP'
        map_column_value['created_by'] = '\'{"id": null, "name": "SYS", "admin": true}\''

        mapping_map_column_value(row)

        out.write(f"({', '.join(map_column_value.values())})")

        seq += 1


with open(output_file, 'w', newline='', encoding='utf-8-sig') as out:
    with open(input_file, newline='', encoding='utf-8-sig') as source:
        out.write(f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n")
        write_values_sql(DictReader(source))
        out.write("\nON CONFLICT ON CONSTRAINT nhi_service_pay_item_unique DO UPDATE SET")
        out.write("\nch_name = EXCLUDED.ch_name,")
        out.write("\nen_name = EXCLUDED.en_name,")
        out.write("\npay_points = EXCLUDED.pay_points,")
        out.write("\neffective_date = EXCLUDED.effective_date,")
        out.write("\nexpiration_date = EXCLUDED.expiration_date,")
        out.write("\norder_type_code = EXCLUDED.order_type_code,")
        out.write("\nnote = EXCLUDED.note,")
        out.write("\nindication = EXCLUDED.indication,")
        out.write("\nmodified_at = CURRENT_TIMESTAMP,")
        out.write("\nmodified_by = '{\"id\": null, \"name\": \"SYS\", \"admin\": true}';")


with open(map_output_file, 'w', newline='', encoding='utf-8-sig') as out:
    with open(map_input_file, newline='', encoding='utf-8-sig') as source:
        out.write(f"INSERT INTO {map_table} ({', '.join(map_columns)}) VALUES\n")
        write_map_values_sql(DictReader(source))
        out.write("\nON CONFLICT ON CONSTRAINT nhi_diagnosis_fee_map_unique DO UPDATE SET")
        out.write("\ninsu_order_code = EXCLUDED.insu_order_code,")
        out.write("\nmax_diagnosis_count = EXCLUDED.max_diagnosis_count,")
        out.write("\nexpiration_date = EXCLUDED.expiration_date,")
        out.write("\nnote = EXCLUDED.note,")
        out.write("\nmodified_at = CURRENT_TIMESTAMP,")
        out.write("\nmodified_by = '{\"id\": null, \"name\": \"SYS\", \"admin\": true}';")

