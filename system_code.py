from csv import DictReader
from uuid import uuid4

input_file = 'system_code/csv/import_data_ALL_SYSTEM_CODE.csv'
output_file = 'system_code/sql/output.sql'

table = 'system_codedata'
column_mapping = {
    'system_code_id': None,
    'seq': None,
    'code_id': {'name': 'codeid', 'handler': 'default_value', 'default': 'NULL'},
    'code_name': {'name': 'codename', 'handler': 'default_value', 'default': 'NULL'},
    'code_group': {'name': 'codegroup', 'handler': 'lower_camel_case', 'default': 'NULL'},
    'code_class1': {'name': 'codeclass1', 'handler': 'strip', 'default': 'NULL'},
    'code_class2': {'name': 'codeclass2', 'handler': 'strip', 'default': 'NULL'},
    'memo': {'name': 'memo', 'handler': 'escape_single_quote', 'default': 'NULL'},
    'visitk_class': {'name': 'visitkind', 'handler': 'default_value', 'default': 'NULL'},
    'sort_value': {'name': 'sort_value', 'handler': 'default_value', 'default': 'NULL'},
    'is_stop': {'name': 'is_stop', 'handler': 'yn_to_bool'},
    'stop_datetime': None,
    'is_group_father': {'name': 'is_group_father', 'handler': 'yn_to_bool'},
    'code_type': {'name': 'codetype', 'handler': 'default_value', 'default': 'NULL'},
    'maintain_type': {'name': 'maintain_type', 'handler': 'default_value', 'default': "'H'"},
    # 'created_at': None,
    # 'created_by': None
}

columns = list(column_mapping.keys())
column_value = {c: 'NULL' for c in columns}


def to_camel_case(snake_str):
    return "".join(x.capitalize() for x in snake_str.lower().split("_"))


def to_lower_camel_case(snake_str):
    # We capitalize the first letter of each component except the first one
    # with the 'capitalize' method and join them together.
    camel_string = to_camel_case(snake_str)
    return snake_str[0].lower() + camel_string[1:]


def handle_default_value(value: str, default_value: str = 'NULL'):
    return f"'{value}'" if value else default_value


def handle_lower_camel_case(value: str):
    return to_lower_camel_case(value)


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
        elif source_cnf['handler'] == 'lower_camel_case':
            column_value[col] = handle_default_value(handle_lower_camel_case(value), default_value)
        elif source_cnf['handler'] == 'strip':
            column_value[col] = handle_default_value(value.strip(), default_value)
        elif source_cnf['handler'] == 'escape_single_quote':
            column_value[col] = handle_default_value(value.strip().replace("'", "''"), default_value)
        elif source_cnf['handler'] == 'yn_to_bool':
            column_value[col] = handle_yn_to_bool(value)

unique_combination = set()

def write_values_sql(reader: DictReader):
    seq = 1
    duplicate_count = 0
    print("Duplicate rows:")
    for index, row in enumerate(reader):
        combination = (row['codeid'], row['codegroup'], row['is_group_father'])
        if combination in unique_combination:
            print(f"{row['codeid']}, {row['codegroup']}, {row['is_group_father']}")
            duplicate_count += 1
            continue
        unique_combination.add(combination)

        if index != 0:
            out.write(",\n")
        # uuid = '10000000-0000-0000-0000-' + str(seq).zfill(12)

        # column_value['system_code_id'] = f"'{uuid4()}'"
        column_value['system_code_id'] = f"default"
        column_value['seq'] = f"default"
        # column_value['organization_id'] = "'10000000-0000-0000-0000-000000000001'"
        column_value['stop_datetime'] = 'NULL'
        # column_value['created_at'] = 'CURRENT_TIMESTAMP'
        # column_value['created_by'] = '\'{"id": null, "name": "SYS", "admin": true}\''

        mapping_column_value(row)

        out.write(f"({', '.join(column_value.values())})")

        seq += 1

    print(f"Total duplicate rows: {duplicate_count}")

with open(output_file, 'w', newline='', encoding='utf-8-sig') as out:
    with open(input_file, newline='', encoding='utf-8-sig') as source:
        out.write(f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n")
        write_values_sql(DictReader(source))
        out.write("\nON CONFLICT ON CONSTRAINT system_codedata_key DO UPDATE SET")
        out.write("\ncode_name = EXCLUDED.code_name,")
        out.write("\ncode_class1 = EXCLUDED.code_class1,")
        out.write("\ncode_class2 = EXCLUDED.code_class2,")
        out.write("\nmemo = EXCLUDED.memo,")
        out.write("\nvisitk_class = EXCLUDED.visitk_class,")
        out.write("\nsort_value = EXCLUDED.sort_value,")
        out.write("\nis_stop = EXCLUDED.is_stop,")
        out.write("\nstop_datetime = EXCLUDED.stop_datetime,")
        out.write("\ncode_type = EXCLUDED.code_type,")
        out.write("\nmaintain_type = EXCLUDED.maintain_type,")
        out.write("\nmodified_at = CURRENT_TIMESTAMP;")
        # out.write("\nmodified_by = '{\"id\": null, \"name\": \"SYS\", \"admin\": true}';")
