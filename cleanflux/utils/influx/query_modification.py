import re
import logging
from pprint import pprint

import cleanflux.utils.influx.query_sqlparsing as influx_query_parsing
import cleanflux.utils.influx.date_manipulation as influx_date_manipulation
import cleanflux.utils.influx.query_sqlparsing as query_sqlparsing


# ------------------------------------------------------------------------
# GLOBALS

change_rp_re = re.compile(r'SELECT.*FROM (?P<measurement>.+?) .*WHERE.*')
group_by_time_re = re.compile(r'^time\((?P<interval>.+?)\)')
change_sum_group_by_time_factor_re = re.compile(r'^(sum|SUM)\(.*?\)(?P<factor>.*?)(( AS | as ).*)?$')
change_sum_group_by_time_factor_with_trans_func_re = re.compile(r'^.*\((\s*)?(sum|SUM)\(.*?\),(.*)\)(?P<factor>.*?)(( AS | as ).*)?$')
lower_time_bound_re = re.compile(r'.*WHERE.* time >=? (?P<lower_time_bound>.+?) (and|AND|GROUP)')
nnd_re = re.compile(r'^(non_negative_derivative|NON_NEGATIVE_DERIVATIVE)\((?P<content>.*?)\)\s*(?P<math_n_alias>.*?)$')
nnd_no_interval_re = re.compile(r'^(non_negative_derivative|NON_NEGATIVE_DERIVATIVE)\((?P<content>.*?),\s*(?P<interval>.+?)\s*\)\s*(?P<math_n_alias>.*?)$')


# ------------------------------------------------------------------------
# QUERY MODIFICATION


def change_rp(schema, rp, measurement, query):

    match = change_rp_re.match(query)

    # schema in query overrides URL param
    measurement_provi = match.groupdict()['measurement']
    parts = measurement_provi.split('.')
    if len(parts) == 3:
        schema = parts[0].replace('"', '')

    return query[0:match.start('measurement')] + '"' + schema \
           + '"."' + rp + '"."' + measurement + '"' + query[match.end('measurement'):]


def change_group_by_time_interval(parsed_query, interval):
    group_by_list = query_sqlparsing.extract_group_by(parsed_query)
    group_by_index = query_sqlparsing.get_token_index_group_by_in_select(parsed_query)

    for i, group_by in enumerate(group_by_list):
        match = group_by_time_re.match(group_by)
        if match:
            group_by_list[i] = group_by[0:match.start('interval')] + interval + group_by[match.end('interval'):]
            break

    parsed_query.tokens[group_by_index] = ', '.join(group_by_list)
    return parsed_query


def change_sum_group_by_time_factor(parsed_query, factor):

    columns = influx_query_parsing.extract_all_columns_in_select(parsed_query)
    columns_token_id = influx_query_parsing.get_token_index_columns_in_select(parsed_query)

    for i, column in enumerate(columns):
        if influx_query_parsing.has_transformation_func_around_sum_for_column(column):
            match = change_sum_group_by_time_factor_with_trans_func_re.match(column)
        else:
            match = change_sum_group_by_time_factor_re.match(column)

        # NB: '*' has precedence over '/'.
        #     E.g.: 60 / 3 * (1 / 4) = (60 / 3) * (1 / 4) = 5
        if match:
            columns[i] = column[0:match.end('factor')] + ' * (' + str(factor) + ')' + column[match.end('factor'):]

    all_columns_str = ', '.join(columns)
    parsed_query.tokens[columns_token_id] = all_columns_str

    return parsed_query


def add_limit(query, limit):
    return query + ' LIMIT ' + str(limit)


def extend_lower_time_bound(query, interval_str):
    match = lower_time_bound_re.match(query)
    lower_time_bound = match.groupdict()['lower_time_bound']
    lower_time_bound = lower_time_bound + " - " + interval_str
    lower_time_bound_start = match.start('lower_time_bound')
    lower_time_bound_end = match.end('lower_time_bound')
    query = query[:lower_time_bound_start] + lower_time_bound + query[lower_time_bound_end:]
    return query


def remove_non_negative_derivative(parsed, index_list=None, forced_column_name_map=None):

    func = 'non_negative_derivative'
    func_strlen = len(func + '(')

    columns = influx_query_parsing.extract_all_columns_in_select(parsed)
    columns_token_id = influx_query_parsing.get_token_index_columns_in_select(parsed)

    index_found = 0
    for i, column in enumerate(columns):
        match_with_interval = nnd_no_interval_re.match(column)
        match_without_interval = nnd_re.match(column)
        if match_with_interval or match_without_interval:
            if index_list is None or index_found in index_list:
                if match_with_interval:
                    new_column = column[0:(match_with_interval.start('content') - func_strlen)] + match_with_interval.groupdict()['content']
                    new_column += column[(match_with_interval.end('interval') + 1):(match_with_interval.end('math_n_alias'))]
                    if forced_column_name_map and index_found in forced_column_name_map:
                        new_column += ' AS ' + forced_column_name_map[index_found] + ' '
                    new_column += column[(match_with_interval.end('math_n_alias')):]
                else:
                    new_column = column[0:(match_without_interval.start('content') - func_strlen)] + match_without_interval.groupdict()[
                        'content']
                    new_column += column[(match_without_interval.end('content') + 1):(match_without_interval.end('math_n_alias'))]
                    if forced_column_name_map and index_found in forced_column_name_map:
                        new_column += ' AS ' + forced_column_name_map[index_found] + ' '
                    new_column += column[(match_without_interval.end('math_n_alias')):]
                columns[i] = new_column
            index_found += 1

    all_columns_str = ', '.join(columns)
    parsed.tokens[columns_token_id] = all_columns_str

    return parsed
