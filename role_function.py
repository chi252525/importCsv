from csv import DictReader
from uuid import uuid4

input_file = 'role_function/import_data_sys_role_function.csv'
output_file = 'role_function/output.sql'

table = 'role_has_permission'
column_mapping = {
    'role_has_permission_id': None,
    'role_id': {'name': 'role_code', 'handler': 'select_role_id', 'default': 'NULL'},
    'permission_id': {'name': 'function_id', 'handler': 'default_value', 'default': 'NULL'},
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


def select_role_id(value: str):
    return f"(SELECT role_id FROM role WHERE code = '{value}')"


def select_permission_id(value: str):
    return f"(SELECT permission_id FROM permission WHERE code = '{value}')"


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
        elif source_cnf['handler'] == 'select_role_id':
            column_value[col] = select_role_id(value)
        elif source_cnf['handler'] == 'select_permission_id':
            column_value[col] = select_permission_id(value)
        elif source_cnf['handler'] == 'yn_to_bool':
            column_value[col] = handle_yn_to_bool(value)


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
            column_value['role_has_permission_id'] = f"'{uuid}'"
            column_value['created_at'] = 'CURRENT_TIMESTAMP'

            mapping_column_value(row)

            code = column_value['permission_id'].replace("'", "")
            code = f"P{code}_{suffix['code']}"
            column_value['permission_id'] = select_permission_id(code)

            out.write(f"({', '.join(column_value.values())})")

            seq += 1


with open(output_file, 'w', newline='', encoding='utf-8-sig') as out:
    with open(input_file, newline='', encoding='utf-8-sig') as source:
        out.write(f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n")
        write_values_sql(DictReader(source))
        out.write("\nON CONFLICT (role_id, permission_id) DO NOTHING;\n")
