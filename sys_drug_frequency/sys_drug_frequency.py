from csv import DictReader
# 請改成可以讀取的路徑
# input_file = 'sys_drug_frequency/csv/sys_drug_frequency.csv'
# output_file = 'sys_drug_frequency/sql/INIT_DATA__sys_drug_frequency.sql'


table = 'sys_drug_frequency'
column_mapping = {
    'sys_drug_frequency_id': {'name': 'uuid', 'handler': 'default_value', 'default': 'NULL'},
    'freq_code': {'name': 'freq_code', 'handler': 'default_value', 'default': 'NULL'},
    'freq_cname': {'name': 'freq_name', 'handler': 'escape_single_quote', 'default': 'NULL'},
    'freq_ename': {'name': 'freq_en_name', 'handler': 'escape_single_quote', 'default': 'NULL'},
    'freq_short_name': {'name': 'freq_short_name', 'handler': 'escape_single_quote', 'default': 'NULL'},
    'freq_insu_code': {'name': 'freq_insu_code', 'handler': 'default_value', 'default': 'NULL'},
    'freq_times': {'name': 'freq_times', 'handler': 'default_value', 'default': 'NULL'},
    'freq_cycle_type': {'name': 'freq_cycle_type', 'handler': 'default_value', 'default': 'NULL'},
    'freq_cycle_num': {'name': 'freq_cycle_num', 'handler': 'default_value', 'default': 'NULL'},
    'give_drug_type': {'name': 'give_drug_type', 'handler': 'default_value', 'default': 'NULL'},
    'sort_value': {'name': 'sort_value', 'handler': 'default_value', 'default': 'NULL'}
}

columns = list(column_mapping.keys())
column_value = {c: 'NULL' for c in columns}

onConflictUpdateSet = []

for col in columns:
    if col == 'sys_drug_frequency_id':
        continue
    onConflictUpdateSet.append(f"{col} = EXCLUDED.{col}")

onConflict = f"ON CONFLICT (sys_drug_frequency_id) DO UPDATE SET {', '.join(onConflictUpdateSet)};"


def handle_default_value(value: str, default_value: str = 'NULL'):
    return f"'{value}'" if value else default_value


def mapping_column_value(row: dict):
    for col in columns:
        source_cnf = column_mapping[col]
        if source_cnf is None:
            continue
        csv_title = source_cnf['name']
        value = row.get(csv_title, '')  # Use get to avoid KeyError
        default_value = source_cnf.get('default')
        if source_cnf['handler'] == 'default_value':
            column_value[col] = handle_default_value(value, default_value)
        elif source_cnf['handler'] == 'escape_single_quote':
            column_value[col] = handle_default_value(value.strip().replace("'", "''"), default_value)


def write_values_sql(reader: DictReader):
    seq = 1
    f = 1
    max_row = 1000

    for index, row in enumerate(reader):
        # if index % (max_row * 1) == 0:
        #     file_name = f"{output_file}_{str(f).zfill(4)}.sql"
        #     out = open(file_name, 'w', newline='', encoding='utf-8')
        #     f += 1

        if index % max_row == 0:
            out.write(f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n")

        if index != 0 and index % max_row != 0:
            out.write(",\n")
        uuid = '10000000-0000-0000-0000-' + str(seq).zfill(12)

        column_value['sys_drug_frequency_id'] = f"'{uuid}'"

        mapping_column_value(row)

        out.write(f"({', '.join(column_value.values())})")
        seq += 1

        if index % max_row == max_row - 1:
            out.write(f"\n{onConflict}\n")

    out.write(f"\n{onConflict}\n")


with open(output_file, 'w', newline='', encoding='utf-8') as out:
    with open(input_file, newline='', encoding='utf-8-sig') as source:
        write_values_sql(DictReader(source))
