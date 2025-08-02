from connectors_to_databases import PostgreSQL


pg = PostgreSQL(
    port=1
)


TABLE = 'dim_kpi_dag_gen_config'


def gen_insert_sql_for_kpi_id(dict_kpi: dict = None) -> str:
    """
    Generates a script for inserting data into SCD.

    Determines if such a key exists.
    If not, it inserts the necessary data specified in dict_kpi.
    If it does exist, it inserts the necessary data specified in dict_kpi and duplicates
    information from previous rows.

    @param dict_kpi: Dictionary describing the KPI.
    @return: A string for inserting values into the SCD.
    """

    # Check for the presence of kpi_id in the table
    df_check = pg.execute_to_df(f'''
    SELECT
        kpi_id
    FROM
        {TABLE}
    WHERE
        kpi_id = {dict_kpi['kpi_id']}
    ''')

    # Verify if such a kpi_id exists in the table
    if len(df_check) >= 1:
        # In the query, exclude the fields that are automatically generated using `DEFAULT`
        query = f'''
            SELECT 
                column_name 
            FROM 
                information_schema.columns 
            WHERE 
                table_name = '{TABLE}'
                AND column_name NOT IN (
                    'id', 'created_at', 'changed_at', 'is_actual', 
                    {', '.join(f"'{i}'" for i in dict_kpi)}
                )
        '''

        df = pg.execute_to_df(query)  # noqa: PD901

        insert_sql_column_current = ', '.join(value for value in df.column_name)
        insert_sql_column_modified = insert_sql_column_current + f''', {', '.join(i for i in dict_kpi)}'''

        list_values = []

        for value in dict_kpi.values():
            # Handling single quotes in values (e.g., in date strings)
            if "'" in str(value):
                value = value.replace("'", "''")
                list_values.append(f"'{value}'")
            elif value is None:
                list_values.append('NULL')
            else:
                list_values.append(f"'{value}'")

        insert_sql_column_values = insert_sql_column_current + f''', {', '.join(list_values)}'''

        sql_insert = f'''
        INSERT INTO {TABLE}
        (
            {insert_sql_column_modified}
        )
        SELECT 
            {insert_sql_column_values} 
        FROM 
            {TABLE}
        WHERE
            is_actual IS TRUE
            AND kpi_id = {dict_kpi['kpi_id']};
        '''
    else:
        # If there is no such kpi_id in the table, generate an insert using the dictionary values
        columns = ', '.join(value for value in dict_kpi)

        list_values = []

        for value in dict_kpi.values():
            if "'" in str(value):
                value = value.replace("'", "''")
                list_values.append(f"'{value}'")
            elif value is None:
                list_values.append('NULL')
            else:
                list_values.append(f"'{value}'")

        values = ', '.join(list_values)

        sql_insert = f'''
            INSERT INTO {TABLE}({columns})
            VALUES ({values});
            '''

    return sql_insert


def scd_dim_kpi(dict_kpi: dict = None) -> None:
    """
    Main function that takes as input a dictionary describing the KPI.

    Each key corresponds to a column in the SCD table.
    Each value corresponds to the value for that column in the SCD table.

    @param dict_kpi: Dictionary describing KPI by selected columns.
    @return: Nothing. Executes the SQL script to insert data into SCD.
    """

    # Update changed_at in the previous active record
    update_changed_at_for_kpi_id = f'''
    UPDATE {TABLE} 
    SET 
        changed_at = NOW() 
    WHERE 
        kpi_id = {dict_kpi['kpi_id']} 
        AND is_actual IS TRUE;
    '''

    # Insert a new record with updated field values
    insert_new_values_for_kpi_id = gen_insert_sql_for_kpi_id(dict_kpi=dict_kpi)

    # Update is_actual for each kpi_id
    update_is_actual_for_kpi_id = f'''
    UPDATE {TABLE} 
    SET 
        is_actual = false 
    WHERE 
        kpi_id = {dict_kpi['kpi_id']} 
        AND id <> (
            SELECT MAX(id) 
            FROM {TABLE} 
            WHERE kpi_id = {dict_kpi['kpi_id']}
        );
    '''

    # Combine the SQL script parts so they execute in one transaction
    sql_query = update_changed_at_for_kpi_id + insert_new_values_for_kpi_id + update_is_actual_for_kpi_id

    print(sql_query)  # noqa: T201
    pg.execute_script(sql_query)


# Example usage
new_values = {
    'kpi_id': 1,
    'dag_id': 'test_1',
    'metric_name_en': 'test_1',
    'owner': 'korsak0v',
    'start_date': '2021-01-01',
    'cron': '10 0 * * *',
    'tags': '''['dm', 'pg', 'gen_dag', 'from_pg']''',
    'sql_query': '''
    SELECT
        date,
        count(values) AS value
    FROM
        fct_some_table_with_random_values
    WHERE
        date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY
        1
    ''',
}


# Function call
scd_dim_kpi(
    dict_kpi=new_values
)
