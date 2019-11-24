'''
Helper functions for parameterizing SQL queries in Python using JinjaSql
'''

from copy import deepcopy

from six import string_types

from jinjasql import JinjaSql

__all__ = ['quote_sql_string', 'get_sql_from_template', 'apply_sql_template','get_column_sql']


def quote_sql_string(value):
    '''
    If `value` is a string type, escapes single quotes in the string
    and returns the string enclosed in single quotes.
    '''
    if isinstance(value, string_types):
        new_value = str(value)
        new_value = new_value.replace("'", "''")
        return "'{}'".format(new_value)
    return value


def get_sql_from_template(query, bind_params):
    '''
    Given a query and binding parameters produced by JinjaSql's prepare_query(),
    produce and return a complete SQL query string.
    '''
    if not bind_params:
        return query
    params = deepcopy(bind_params)
    for key, val in params.items():
        params[key] = quote_sql_string(val)
    return query % params


def apply_sql_template(template, parameters):
    '''
    Apply a JinjaSql template (string) substituting parameters (dict) and return
    the final SQL.
    '''
    j = JinjaSql(param_style='pyformat')
    query, bind_params = j.prepare_query(template, parameters)
    return get_sql_from_template(query, bind_params)


MIN_MAX_VALUE_TEMPLATE = '''
select
    min({{ column_name | sqlsafe }}) as min_value,
    max({{ column_name | sqlsafe }}) as max_value
from
    {{ table_name | sqlsafe }}
'''

MIN_MAX_LENGTH_TEMPLATE = '''
select
    min(length({{ column_name | sqlsafe }})) as min_length_value,
    max(length({{ column_name | sqlsafe }})) as max__length_value
from
    {{ table_name | sqlsafe }}
'''

NOT_NULL_TEMPLATE = '''
select
    count({{ column_name | sqlsafe }}) as not_null
from
    {{ table_name | sqlsafe }}
where {{ column_name | sqlsafe }} is not null
'''

UNIQUE_VAL_SINGLE_COL_TEMPLATE = '''
select
    {{ column_name | sqlsafe }}, COUNT(*) as COUNT
from
    {{ table_name | sqlsafe }}
group by {{ column_name | sqlsafe }}
having count(*) > 1
'''

GET_BUILD_SCHEMA_TEMPLATE = '''
select 
    column_name,
    case when data_type='VARCHAR2' then 'String' 
        when data_type='CHAR' then 'String' 
        when data_type='DATE' then 'Date' 
        when data_type='NVARCHAR2' then 'String'
        when data_type='NUMBER' then 'Integer' 
        else 'String' end AS data_type,
        case when nullable = 'N' then 'False' else 'True' end  as nullable
from dba_tab_columns
where table_name =  '{{ table_name | sqlsafe }}' 
order by column_id asc
'''




def get_column_sql(table_name, column_name, default_value):
    params = {
        'table_name': table_name,
        'column_name': column_name,
        'default_value': default_value,
    }
    return apply_sql_template(MIN_MAX_VALUE_TEMPLATE, params)

def get_unique_sql(table_name, column_name, default_value):
    params = {
        'table_name': table_name,
        'column_name': column_name,
        'default_value': default_value,
    }
    return apply_sql_template(UNIQUE_VAL_SINGLE_COL_TEMPLATE, params)

def get_min_max_value_sql(table_name, column_name, default_value):
    params = {
        'table_name': table_name,
        'column_name': column_name,
        'default_value': default_value,
    }
    return apply_sql_template(MIN_MAX_VALUE_TEMPLATE, params)

def get_min_max_length_sql(table_name, column_name, default_value):
    params = {
        'table_name': table_name,
        'column_name': column_name,
        'default_value': default_value,
    }
    return apply_sql_template(MIN_MAX_LENGTH_TEMPLATE, params)

def get_build_schema(table_name):
    params = {
        'table_name': table_name,
    }
    return apply_sql_template(GET_BUILD_SCHEMA_TEMPLATE, params)