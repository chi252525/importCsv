from csv import DictReader

input_file = 'diag_icd10/csv/DIAG_ICD10.csv'
output_file = 'diag_icd10/sql/INIT_DATA__diag_icd10.sql'

table = 'diag_icd10'
column_mapping = {
    'diag_idc10_id': None,
    'icd10_code': {'name': 'icd10_code', 'handler': 'default_value', 'default': 'NULL'},
    'icd10_cname': {'name': 'icd10_cname', 'handler': 'escape_single_quote', 'default': 'NULL'},
    'icd10_ename': {'name': 'icd10_ename', 'handler': 'escape_single_quote', 'default': 'NULL'},
    'icd10_hosp_name': {'name': 'icd10_hosp_name', 'handler': 'escape_single_quote', 'default': 'NULL'},
    'icd10_insu_code': {'name': 'icd10_insu_code', 'handler': 'default_value', 'default': 'NULL'},
    'icd10_type': {'name': 'icd10_type', 'handler': 'default_value', 'default': 'NULL'},
    'icd10_kind': {'name': 'icd10_kind', 'handler': 'default_value', 'default': 'NULL'},
    'icd10_agetype': {'name': 'icd10_agetype', 'handler': 'default_value', 'default': 'NULL'},
    'icd10_gender_type': {'name': 'icd10_gender_type', 'handler': 'default_value', 'default': 'NULL'},
    'icd10_short_code': {'name': 'icd10_short_code', 'handler': 'default_value', 'default': 'NULL'},
    'icd10_short_name': {'name': 'icd10_short_name', 'handler': 'default_value', 'default': 'NULL'},
    'visit_class': {'name': 'visit_class', 'handler': 'default_value', 'default': 'NULL'},
    'icd10_major_code_o': {'name': 'icd10_major_code_o', 'handler': 'default_value', 'default': 'NULL'},
    'icd10_major_code_i': {'name': 'icd10_major_code_i', 'handler': 'default_value', 'default': 'NULL'},
    'admit_days': {'name': 'admit_days', 'handler': 'default_value', 'default': 'NULL'},
    'heavysick_flag': {'name': 'heavysick_flag', 'handler': 'default_value', 'default': 'NULL'},
    'heavysick_class': {'name': 'heavysick_class', 'handler': 'default_value', 'default': 'NULL'},
    'check_discount_flag': {'name': 'check_discount_flag', 'handler': 'default_value', 'default': 'NULL'},
    'slowflag': {'name': 'slowflag', 'handler': 'default_value', 'default': 'NULL'},
    'rough_flag': {'name': 'rough_flag', 'handler': 'default_value', 'default': 'NULL'},
    'rough_icd10_code': {'name': 'rough_icd10_code', 'handler': 'default_value', 'default': 'NULL'},
    'group_class': {'name': 'group_class', 'handler': 'default_value', 'default': 'NULL'},
    'section_no': {'name': 'section_no', 'handler': 'default_value', 'default': 'NULL'},
    'group_no': {'name': 'group_no', 'handler': 'default_value', 'default': 'NULL'},
    'rare': {'name': 'rare', 'handler': 'default_value', 'default': 'NULL'},
    'initialcare': {'name': 'initialcare', 'handler': 'default_value', 'default': 'NULL'},
    'virtual': {'name': 'virtual', 'handler': 'default_value', 'default': 'NULL'},
    'note_period': {'name': 'note_period', 'handler': 'default_value', 'default': 'NULL'},
    'infection_type': {'name': 'infection_type', 'handler': 'default_value', 'default': 'NULL'},
    'startdate': {'name': 'startdate', 'handler': 'default_value', 'default': 'NULL'},
    'enddate': {'name': 'enddate', 'handler': 'default_value', 'default': 'NULL'},
    'vermark': {'name': 'vermark', 'handler': 'default_value', 'default': 'NULL'},
    'cc': {'name': 'cc', 'handler': 'default_value', 'default': 'NULL'},
    'id_maincu': {'name': 'id_maincu', 'handler': 'default_value', 'default': 'NULL'},
    'insu_pay': {'name': 'insu_pay', 'handler': 'default_value', 'default': 'NULL'},
    'id_tipmsg': {'name': 'id_tipmsg', 'handler': 'default_value', 'default': 'NULL'},
    'new_startdate': {'name': 'new_startdate', 'handler': 'default_value', 'default': 'NULL'},
    'new_stop_flag': {'name': 'new_stop_flag', 'handler': 'default_value', 'default': 'NULL'},
    'new_diag_cname': {'name': 'new_diag_cname', 'handler': 'default_value', 'default': 'NULL'},
    'new_diag_ename': {'name': 'new_diag_ename', 'handler': 'default_value', 'default': 'NULL'},
    'id_job_cd': {'name': 'id_job_cd', 'handler': 'default_value', 'default': 'NULL'},
    'oldcode': {'name': 'oldcode', 'handler': 'default_value', 'default': 'NULL'},
    'stopemp_no': {'name': 'stopemp_no', 'handler': 'default_value', 'default': 'NULL'},
    'stop_flag': {'name': 'stop_flag', 'handler': 'default_value', 'default': 'NULL'},
    'id_anti_check': {'name': 'id_anti_check', 'handler': 'default_value', 'default': 'NULL'},
    'state': {'name': 'state', 'handler': 'default_value', 'default': 'NULL'},
    'mp_icd9code': {'name': 'mp_icd9code', 'handler': 'default_value', 'default': 'NULL'},
    'sickid': {'name': 'sickid', 'handler': 'default_value', 'default': 'NULL'},
    'acode': {'name': 'acode', 'handler': 'default_value', 'default': 'NULL'},
    'surgerycode': {'name': 'surgerycode', 'handler': 'default_value', 'default': 'NULL'}
}

columns = list(column_mapping.keys())
column_value = {c: 'NULL' for c in columns}

onConflictUpdateSet = []

for col in columns:
    if col == 'diag_idc10_id':
        continue
    onConflictUpdateSet.append(f"{col} = EXCLUDED.{col}")

onConflict = f"ON CONFLICT (diag_idc10_id) DO UPDATE SET {', '.join(onConflictUpdateSet)};"


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

        column_value['diag_idc10_id'] = f"'{uuid}'"

        mapping_column_value(row)

        out.write(f"({', '.join(column_value.values())})")
        seq += 1

        if index % max_row == max_row - 1:
            out.write(f"\n{onConflict}\n")

    out.write(f"\n{onConflict}\n")


with open(output_file, 'w', newline='', encoding='utf-8') as out:
    with open(input_file, newline='', encoding='utf-8-sig') as source:
        write_values_sql(DictReader(source))
