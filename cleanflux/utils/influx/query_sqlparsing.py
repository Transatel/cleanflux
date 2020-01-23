import re
import logging
from pprint import pprint
import sqlparse
from datadog import statsd


import cleanflux.utils.influx.date_manipulation as influx_date_manipulation


# https://github.com/andialbrecht/sqlparse/blob/master/examples/extract_table_names.py
# Example 10 in https://www.programcreek.com/python/example/101324/sqlparse.sql.Function


# ------------------------------------------------------------------------
# GLOBALS

nnd_interval_re = re.compile(r'.*(non_negative_derivative|NON_NEGATIVE_DERIVATIVE)\(.*,\s*(?P<interval>.+?)\)\s?')
nnd_column_name_re = re.compile(r'.*(non_negative_derivative|NON_NEGATIVE_DERIVATIVE)\((?P<aggreg_func>.*?)\((?P<content>.*?)\).*?\s*(as|AS)\s*(?P<as>.+?)$')
lower_time_bound_re = re.compile(r'.*WHERE.* time >=? (?P<lower_time_bound>.+?) (and|AND|GROUP)')
lower_time_bound_absolute_re = re.compile(r'SELECT.*WHERE.*time >=? (?P<from>.+?) .*')
lower_time_bound_relative_re = re.compile(r'SELECT.*WHERE.*time >=? now\(\) - (?P<from>.+?) .*')

upper_time_bound_absolute_re = re.compile(r'SELECT.*WHERE.*time <=? (?P<to>.+?) .*')
upper_time_bound_relative_re = re.compile(r'SELECT.*WHERE.*time <=? now\(\) - (?P<to>.+?) .*')
upper_time_bound_is_now_re = re.compile(r'SELECT.*WHERE.*time <=? now\(\) .*')


# ------------------------------------------------------------------------
# INITIAL PARSING

@statsd.timed('timer_sqlparse_query', use_ms=True)
def sqlparse_query(query):
    # type: (str) -> sqlparse.sql.Statement
    parsed = sqlparse.parse(query)
    return parsed[0]


@statsd.timed('timer_stringify_sqlparsed', use_ms=True)
def stringify_sqlparsed(parsed):
    query = ''
    for token in parsed.tokens:
        if isinstance(token, bytes) or isinstance(token, str):
            query += token
        else:
            query += token.value
    return query


# ------------------------------------------------------------------------
# TESTS: QUERY TYPE

def is_select(parsed):
    # type: (sqlparse.sql.Statement) -> bool
    return parsed.get_type() == 'SELECT'


def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is sqlparse.tokens.DML and item.value.upper() == 'SELECT':
            return True
    return False


# ------------------------------------------------------------------------
# SELECT FIELDS FUNCTIONS

def dict_get_path(my_dict, path, default_value=None):
    split_path = path.split('.')
    try:
        for p in split_path:
            if re.match(r'^\[\d*\]$', p):
                p = int(p[1:-1])
            my_dict = my_dict[p]
    except KeyError:
        return default_value
    except IndexError:
        return default_value
    return my_dict


def dict_set_path(my_dict, path, value):
    split_path = path.split('.')
    for key in split_path[:-1]:
        my_dict = my_dict.setdefault(key, {})
    my_dict[split_path[-1]] = value


def parse_function_args(func_args):
    func_args = func_args.strip()

    args_list = []
    current_arg = ''
    current_str_quote = None
    prev_c = None
    for c in func_args:
        if c in ['"', '\'']:
            if not current_str_quote:
                current_str_quote = c
            elif c == current_str_quote and prev_c != '\\':
                current_str_quote = None
            current_arg += c
        elif not current_str_quote and c == ',':
            args_list.append(current_arg.strip())
            current_arg = ''
        else:
            current_arg += c
        # NB: we only use prev_c to test whether escaping quote
        # we do this hack to handle the case of double escaping for the literal '\' character
        if prev_c == '\\' and c == '\\':
            prev_c = None
        else:
            prev_c = c

    # TODO: need to handle case previous char was ',' or not
    # testing if has crossed an unquoted ',' at least once should be sufficient
    if current_arg.strip():
        args_list.append(current_arg.strip())

    if args_list:
        return args_list
    if func_args:
        return [func_args]
    return []


def parse_function_call(func_call):
    parsed = {}
    current_keyword = ''
    current_path = ''
    # tree_level = 0
    # TODO: should handle quoting at this level as well
    for c in func_call:
        if c == '(':
            # tree_level += 1
            if current_path:
                current_path += '.'
            current_path += current_keyword
            current_keyword = ''
        elif c == ')':
            if current_keyword:
                dict_set_path(parsed, current_path, parse_function_args(current_keyword))
            current_path = '.'.join(current_path.split('.')[:-1])
            current_keyword = ''
        else:
            current_keyword += c

    if parsed:
        return parsed
    return func_call


def extract_all_columns_in_select(parsed):
    columns = []
    for token in parsed.tokens:
        if isinstance(token, sqlparse.sql.Function):
            # simple case
            columns.append(token.value)
        elif isinstance(token, sqlparse.sql.Identifier):
            # more complex statement, such as one with an 'AS'
            columns.append(token.value)
        elif isinstance(token, sqlparse.sql.Operation):
            # more complex statement, such as one with a math operation
            columns.append(token.value)
        elif isinstance(token, sqlparse.sql.IdentifierList):
            # complex statements, several columns returned
            columns = split_columns(token.value)
        elif isinstance(token, bytes) or isinstance(token, str):
            # previously reworked column
            columns = split_columns(token)
        elif token.ttype == sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
            # at end of field list
            return columns


def get_token_index_columns_in_select(parsed):
    for i, token in enumerate(parsed.tokens):
        if isinstance(token, sqlparse.sql.Function) \
                or isinstance(token, sqlparse.sql.Identifier) \
                or isinstance(token, sqlparse.sql.Operation) \
                or isinstance(token, sqlparse.sql.IdentifierList) \
                or isinstance(token, bytes) \
                or isinstance(token, str):
            return i


def extract_function_in_select(parsed, func, even_wrapped=False):
    func_calls = []
    func = func.upper()
    columns = extract_all_columns_in_select(parsed)
    for column in columns:
        column = str(column.upper())
        if column.startswith(func + '('):
            func_calls.append(column)
        elif even_wrapped and '(' + func + '(' in column:
            # TODO: replace matching test w/ a regexp
            func_calls.append(column)
    return func_calls


def has_function_in_select(parsed, func, even_wrapped=False):
    func_calls = extract_function_in_select(parsed, func, even_wrapped)
    if func_calls:
        return True
    return False


def has_sum_in_select(parsed):
    return has_function_in_select(parsed, 'sum', True)


# TODO: handle quoted / escaped '(', ')' and ','
def split_columns(all_columns_str):
    level = 0
    curr_col_start = 0
    columns = []
    i = 0
    for i, c in enumerate(all_columns_str):
        if c == '(':
            level += 1
        elif c == ')':
            level -= 1
        elif c == ',':
            if level == 0:
                columns.append(all_columns_str[curr_col_start:i])
                curr_col_start = i + 1
    columns.append(all_columns_str[curr_col_start:i+1])
    columns = [col.strip() for col in columns]
    return columns


# ------------------------------------------------------------------------
# MEASUREMENT

def extract_from_helper(parsed, mode):
    has_from = False
    has_whitespace = False
    for i, token in enumerate(parsed.tokens):
        if has_whitespace:
            # if isinstance(token, sqlparse.sql.Identifier):
            if isinstance(token, bytes) \
               or isinstance(token, str) \
               or isinstance(token, sqlparse.sql.Token):
                if mode == "value":
                    if isinstance(token, sqlparse.sql.Token):
                        return token.value
                    else:
                        return token
                else:
                    return i
            else:
                has_from = False
                has_whitespace = False
        if has_from:
            if token.ttype == sqlparse.tokens.Whitespace:
                has_whitespace = True
            else:
                has_from = False
            continue
        if isinstance(token, bytes) or isinstance(token, str):
            # reworked columns
            continue
        if token.ttype == sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
            has_from = True
            continue


def extract_measurement_from_query(schema, parsed):
    measurement = extract_from_helper(parsed, "value")

    if not measurement:
        return None

    parts = measurement.split('.')

    new_parts = []
    prev_part = None
    for part in parts:
        if part[0] == '"' and part[len(part) - 1] == '"':
            new_parts.append(part)
        else:
            if part[0] == '"':
                prev_part = part
            elif part[len(part) - 1] == '"':
                new_parts.append(prev_part + '.' + part)
                prev_part = None
            else:
                if prev_part is None:
                    new_parts.append(part)
                else:
                    prev_part += '.' + part
    parts = new_parts

    if len(parts) > 3:
        return None
    if len(parts) == 3:
        return {'schema': parts[0].replace('"', ''), 'rp': parts[1].replace('"', ''),
                'measurement': parts[2].replace('"', '')}
    elif len(parts) == 2:
        return {'schema': schema, 'rp': parts[0].replace('"', ''), 'measurement': parts[1].replace('"', '')}
    else:
        return {'schema': schema, 'rp': None, 'measurement': parts[0].replace('"', '')}


# ------------------------------------------------------------------------
# GROUP BY

def extract_group_by_helper(parsed, mode):
    has_group = False
    has_1rst_whitespace = False
    has_2nd_whitespace = False
    has_by = False
    for i, token in enumerate(parsed.tokens):
        if hasattr(token, 'ttype') and token.ttype == sqlparse.tokens.Keyword and token.value.upper() == 'GROUP':
            has_group = True
            continue
        if hasattr(token, 'ttype') and token.ttype == sqlparse.tokens.Keyword and token.value.upper() == 'GROUP BY':
            has_group = True
            has_1rst_whitespace = True
            has_by = True
            continue
        if has_group:
            if not has_1rst_whitespace:
                if token.ttype == sqlparse.tokens.Whitespace:
                    has_1rst_whitespace = True
                    continue
                else:
                    return None
        if has_1rst_whitespace:
            if not has_by:
                if token.ttype == sqlparse.tokens.Keyword and token.value.upper() == 'BY':
                    has_by = True
                    continue
                else:
                    return None
        if has_by:
            if not has_2nd_whitespace:
                if token.ttype == sqlparse.tokens.Whitespace:
                    has_2nd_whitespace = True
                    continue
                else:
                    return None
        if has_2nd_whitespace:
            if isinstance(token, sqlparse.sql.IdentifierList):
                if mode == "value":
                    by = str(token.value).split(',')
                    by = list(map(str.strip, by))
                    return by
                else:
                    return i
            elif isinstance(token, bytes) or isinstance(token, str):
                # case only GROUP BY time()
                if mode == "value":
                    by = [ token ]
                    return by
                else:
                    return i
            elif isinstance(token, sqlparse.sql.Function):
                # case only GROUP BY time()
                if mode == "value":
                    by = [ token.value ]
                    return by
                else:
                    return i
            else:
                return None


def extract_group_by(parsed):
    return extract_group_by_helper(parsed, 'value')


def get_token_index_group_by_in_select(parsed):
    return extract_group_by_helper(parsed, 'index')


def is_grouped_by_time(parsed):
    group_by = extract_group_by(parsed)
    if not group_by:
        return False
    group_by_time = [group_cond for group_cond in group_by if group_cond.startswith('time(')]
    if group_by_time:
        return True
    else:
        return False


def extract_time_interval_group_by(parsed):
    group_by_list = extract_group_by(parsed)
    group_by_time = [group_cond for group_cond in group_by_list if group_cond.startswith('time(')]
    if not group_by_time:
        return None
    else:
        group_by_time = group_by_time[0]
        match = re.match(r'time\((?P<interval>.+?)\)', group_by_time)
        if match:
            return match.groupdict()['interval'].strip()
        else:
            return None


# ------------------------------------------------------------------------
# USUAL QUERY TYPES

def is_sum_group_by_time(parsed):
    # type: (sqlparse.sql.Statement) -> bool
    return has_sum_in_select(parsed) and is_grouped_by_time(parsed)


def is_non_negative_difference(parsed):
    # type: (sqlparse.sql.Statement) -> bool
    return has_function_in_select(parsed, 'non_negative_difference') and is_grouped_by_time(parsed)


def is_non_negative_derivative(parsed):
    return has_function_in_select(parsed, 'non_negative_derivative') and is_grouped_by_time(parsed)


def extract_non_negative_derivative_time_interval(parsed):
    func_calls = extract_function_in_select(parsed, 'non_negative_derivative')
    if not func_calls:
        return None

    intervals = []
    for call in func_calls:

        match = nnd_interval_re.match(call)
        if match:
            intervals.append(match.groupdict()['interval'].strip())
        else:
            intervals.append('1s')

    return intervals


def extract_non_negative_derivative_column_name(parsed):
    func_calls = extract_function_in_select(parsed, 'non_negative_derivative')
    if not func_calls:
        return None

    column_names = []
    for call in func_calls:
        match = nnd_column_name_re.match(call)
        if match:
            column_names.append(match.groupdict()['as'].strip())
        else:
            column_names.append('non_negative_derivative')

    return column_names


def has_transformation_func_around_sum_for_column(column):
    transformation_function_list = [
        'spread',
        'derivative', 'non_negative_derivative',
        'difference', 'non_negative_difference',
        'moving_average',
        'cumulative_sum',
        'stddev',
        'elasped',
    ]
    transformation_function_list += [func.upper() for func in transformation_function_list]
    transformation_function_rx = '(' + '|'.join(transformation_function_list) + ')'
    rx = r'' + transformation_function_rx + r'\((\s*)?(sum|SUM)'
    match = re.match(rx, column)
    if match:
        return True
    else:
        return False


def has_transformation_func_around_sum(parsed):
    func_calls = extract_function_in_select(parsed, 'non_negative_derivative')
    if not func_calls:
        return None

    result_list = []
    for call in func_calls:
        result_list.append(has_transformation_func_around_sum_for_column(call))

    return result_list


# ------------------------------------------------------------------------
# TIME BOUNDS

def is_lower_time_bound_parsable(query):
    match = lower_time_bound_re.match(query)
    if match:
        return True
    else:
        return False


def extract_lower_time_bound_old(query):
    match = lower_time_bound_re.match(query)
    if match:
        return match.groupdict()['lower_time_bound']


def extract_lower_time_bound(query):
    from_time = None
    match_from_interval = lower_time_bound_relative_re.match(query)
    if match_from_interval:
        from_time_interval = match_from_interval.groupdict()['from']
        from_timedelta = influx_date_manipulation.influx_interval_to_timedelta(from_time_interval)
        from_time = influx_date_manipulation.apply_timedelta_on_now(-1 * from_timedelta)
    else:
        match_from = lower_time_bound_absolute_re.match(query)
        if match_from:
            from_time = match_from.groupdict()['from']
            from_time = influx_date_manipulation.influx_timestamp_to_datetime(from_time)
    return from_time


def extract_upper_time_bound(query):
    to_time = None
    match_to_interval = upper_time_bound_relative_re.match(query)
    match_to_interval_2 = upper_time_bound_is_now_re.match(query)
    if match_to_interval:
        to_time_interval = match_to_interval.groupdict()['to']
        to_timedelta = influx_date_manipulation.influx_interval_to_timedelta(to_time_interval)
        to_time = influx_date_manipulation.apply_timedelta_on_now(-1 * to_timedelta)
    elif match_to_interval_2:
        return None
    else:
        match_to = upper_time_bound_absolute_re.match(query)
        if match_to:
            to_time = match_to.groupdict()['to']
            to_time = influx_date_manipulation.influx_timestamp_to_datetime(to_time)
    return to_time


def extract_time_window_bounds(query):
    from_time = extract_lower_time_bound(query)
    to_time = extract_upper_time_bound(query)
    return {'from': from_time, 'to': to_time}


def get_query_time_window(query):
    time_bounds = extract_time_window_bounds(query)
    if time_bounds['from'] is None:
        logging.info('no lower time boundary in query, cannot select automatically RP')
        return None
    if time_bounds['to'] is None:
        time_bounds['to'] = influx_date_manipulation.get_now_datetime()
    query_window_timedelta = time_bounds['to'] - time_bounds['from']
    return query_window_timedelta
