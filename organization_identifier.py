from csv import DictReader
from uuid import uuid4

input_file = 'organization_identifier/csv/hospbsc.csv'
output_file = 'organization_identifier/sql/output.sql'

table = 'organization_identifier'
column_mapping = {
    'organization_identifier_id': None,
    'seq': None,
    'district': {'name': '分區別', 'handler': 'default_value', 'default': 'NULL'},
    'code': {'name': '醫事機構代碼', 'handler': 'default_value', 'default': 'NULL'},
    'name': {'name': '醫事機構名稱', 'handler': 'default_value', 'default': 'NULL'},
    'address': {'name': '機構地址', 'handler': 'default_value', 'default': 'NULL'},
    'phone_area_code': {'name': '電話區域號碼 ', 'handler': 'default_value', 'default': 'NULL'},
    'phone_number': {'name': '電話號碼', 'handler': 'default_value', 'default': 'NULL'},
    'contract_type': {'name': '特約類別', 'handler': 'default_value', 'default': 'NULL'},
    'category_type': {'name': '型態別', 'handler': 'default_value', 'default': 'NULL'},
    'organization_type': {'name': '醫事機構種類', 'handler': 'default_value', 'default': 'NULL'},
    'termination_date': {'name': '終止合約或歇業日期', 'handler': 'default_value', 'default': 'NULL'},
    'operating_status': {'name': '開業狀況', 'handler': 'default_value', 'default': 'NULL'},
    'original_contract_start_date': {'name': '原始合約起日', 'handler': 'default_value', 'default': 'NULL'}
}

columns = list(column_mapping.keys())
column_value = {c: 'NULL' for c in columns}


def handle_default_value(value: str, default_value: str = 'NULL'):
    return f"'{value}'" if value else default_value


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


def write_values_sql(reader: DictReader):
    seq = 1
    for index, row in enumerate(reader):
        if index != 0:
            out.write(",\n")
        uuid = '10000000-0000-0000-0000-' + str(seq).zfill(12)
        # column_value['organization_identifier_id'] = f"'{uuid4()}'"
        column_value['organization_identifier_id'] = f"'{uuid}'"
        column_value['seq'] = f"{seq}"

        mapping_column_value(row)
        out.write(f"({', '.join(column_value.values())})")

        seq += 1


with open(output_file, 'w', newline='', encoding='utf-8-sig') as out:
    with open(input_file, newline='', encoding='utf-8-sig') as source:
        out.write(f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n")
        write_values_sql(DictReader(source))
        out.write("\nON CONFLICT ON CONSTRAINT organization_identifier_pkey DO NOTHING;\n")
