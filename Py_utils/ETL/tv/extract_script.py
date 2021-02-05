class Extract_HQL_Const:

    nearest_to_enddate_in_date_range = '''
        SELECT max(t.file_name) as file_name from {db}.{table} t where t.file_name <= {end_date} and  t.file_name >={start_date};
    '''

    nearest_to_enddate_after_end_date = '''
        SELECT min(t.file_name) as file_name from  {db}.{table} t where t.file_name >= {end_date};
    '''

    tivo_files_exists_in_range = '''
        select file_name as file_names from {db}.{table} where  file_name >= {start_date} 
        and file_name <= {end_date}  group by file_name;
    '''

    is_kantar_week_exist = '''
        select week from {db}.{table} where year = {year} and week = {week};
    '''

    max_kantar_week_exist = '''
        select max(week) as week from {db}.{table} where year = {year};
    '''

    kantar_weeks_exists_in_range = '''
        select week as weeks from {db}.{table} where year >= {start_year} and year <= {end_year}
            and week  >= {start_week} and week <= {end_week}  group by week;
    '''

    max_experian_data_exist = '''
        select max(file_name) as file_name from {db}.{table};
    '''
