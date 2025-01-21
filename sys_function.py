from csv import DictReader
from uuid import uuid4
import math

input_file = 'sys_function/import_data_sys_functions.csv'
output_sql_file = 'sys_function/output.sql'
output_java_file = 'sys_function/output.java'

table = 'permission'
column_mapping = {
    'permission_id': None,
    'seq': None,
    'system': None,
    'code': {'name': 'function_id', 'handler': 'strip', 'default': 'NULL'},
    'bit_index': None,
    'name': {'name': 'function_cname', 'handler': 'strip', 'default': 'NULL'},
    'description': {'name': 'function_cname', 'handler': 'strip', 'default': 'NULL'},
    'enabled_at': None,
    'locked_at': None,
    'created_at': None,
    'created_by': None
}

permission_code_suffix = [
    {'code': 'SEARCH', 'name': '查询'},
    {'code': 'CREATE', 'name': '新增'},
    {'code': 'UPDATE', 'name': '修改'},
    {'code': 'DELETE', 'name': '删除'},
    {'code': 'EXPORT', 'name': '导出'},
    {'code': 'IMPORT', 'name': '导入'},
    {'code': 'SUSPEND', 'name': '停用'},
]

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
        elif source_cnf['handler'] == 'strip':
            column_value[col] = handle_default_value(value.strip(), default_value)


def write_values_sql(reader: DictReader):
    seq = 1
    for index, row in enumerate(reader):
        if index != 0:
            out.write(",\n")
        for suffix_index, suffix in enumerate(permission_code_suffix):
            if suffix_index != 0:
                out.write(",\n")

            uuid = '10000000-0000-0000-0000-' + str(seq).zfill(12)
            # column_value['organization_identifier_id'] = f"'{uuid4()}'"
            column_value['permission_id'] = f"'{uuid}'"
            column_value['seq'] = f"{seq}"
            column_value['system'] = "2"
            column_value['bit_index'] = f"{seq}"
            column_value['enabled_at'] = 'CURRENT_TIMESTAMP'
            column_value['created_at'] = 'CURRENT_TIMESTAMP'

            mapping_column_value(row)

            code = column_value['code'].replace("'", "")
            column_value['code'] = f"'P{code}_{suffix['code']}'"

            name = column_value['name'].replace("'", "")
            column_value['name'] = f"'{name} {suffix['name']}'"
            column_value['description'] = column_value['name']

            out.write(f"({', '.join(column_value.values())})")

            seq += 1


def write_values_java(reader: DictReader):
    seq = 1
    for index, row in enumerate(reader):
        for suffix_index, suffix in enumerate(permission_code_suffix):
            code = row['function_id']
            enum_name = f"P{code}_{suffix['code']}"

            bitIndex = f"{seq}"

            description = f"\"{row['function_cname']}\""

            num: int = int(math.pow(2, seq % 63))

            out_java.write(f"{enum_name}({bitIndex}, {description}), // {num}\n")

            seq += 1


with open(input_file, newline='', encoding='utf-8-sig') as source:
    with open(output_sql_file, 'w', newline='', encoding='utf-8-sig') as out:
        out.write(f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n")
        write_values_sql(DictReader(source))
        out.write("\nON CONFLICT (permission_id) DO NOTHING;\n")

with open(input_file, newline='', encoding='utf-8-sig') as source:
    with open(output_java_file, 'w', newline='', encoding='utf-8-sig') as out_java:
        write_values_java(DictReader(source))
