import logging
import math
import re
from datetime import timedelta
from pprint import pprint
from datadog import statsd

import cleanflux.utils.influx.query_sqlparsing as influx_query_parsing
import cleanflux.utils.influx.query_modification as influx_query_modification
import cleanflux.utils.influx.date_manipulation as influx_date_manipulation
import cleanflux.utils.influx.rp_conf_access as influx_rp_conf_access
import cleanflux.utils.influx.querying_spe as influx_querying_spe


# ------------------------------------------------------------------------
# PUBLIC

@statsd.timed('timer_update_query_with_right_rp', use_ms=True)
def update_query_with_right_rp(from_parts, query, parsed_query,
                               known_retention_policies, aggregation_properties,
                               override_explicit_rp=False):
    output = get_right_rp_for_query(from_parts['schema'], query, parsed_query, known_retention_policies, override_explicit_rp)

    if output is None:
        return None

    counter_aggregation_mode = get_counter_aggregation_mode(from_parts, aggregation_properties)
    if counter_aggregation_mode is None:
        counter_aggregation_mode = 'mean'

    is_changed = False

    if 'rp' in output:
        from_id = influx_query_parsing.extract_from_helper(parsed_query, 'index')
        parsed_query.tokens[from_id] = '"' + from_parts['schema'] + '"."' + output['rp'] + '"."' + from_parts['measurement'] + '"'
        is_changed = True
    if 'group_by_time_interval' in output:
        parsed_query = influx_query_modification.change_group_by_time_interval(parsed_query,
                                                                               output['group_by_time_interval'])
        is_changed = True
    if 'sum_group_by_time_interval_factor' in output:
        if counter_aggregation_mode == 'sum':
            parsed_query = influx_query_modification.change_sum_group_by_time_factor(
                parsed_query,
                output['sum_group_by_time_interval_factor'])
            is_changed = True

    if is_changed:
        query = influx_query_parsing.stringify_sqlparsed(parsed_query)

    from_parts['rp'] = output['rp']

    logging.info('Reworked query (auto RP): ' + query)

    return query


@statsd.timed('timer_update_query_to_limit_nb_points_per_series', use_ms=True)
def update_query_to_limit_nb_points_per_series(from_parts, query, parsed_query,
                                               aggregation_properties, max_nb_points_per_series):

    schema = from_parts['schema']

    # counter_aggregation_mode = get_counter_aggregation_mode(from_parts, aggregation_properties)
    # if counter_aggregation_mode is None:
    #     counter_aggregation_mode = 'mean'

    expected_nb_points_per_series = get_expected_nb_points_per_series(query, parsed_query)
    if expected_nb_points_per_series is None:
        return None
    if expected_nb_points_per_series['nb_points'] > max_nb_points_per_series:
        logging.info('Expected nb of points per series ' + str(
            expected_nb_points_per_series['nb_points']) + ' is bigger than max allowed one (' + str(
            max_nb_points_per_series) + ')')

        my_factor = expected_nb_points_per_series['nb_points'] // max_nb_points_per_series
        split_group_by_time_interval = influx_date_manipulation.split_influx_time(
            expected_nb_points_per_series['group_by_time_interval'])
        adjusted_group_by_time_value = int(math.ceil(my_factor * split_group_by_time_interval['number']))
        new_group_by_time_interval = str(adjusted_group_by_time_value) + split_group_by_time_interval['unit']

        parsed_query = influx_query_modification.change_group_by_time_interval(parsed_query, new_group_by_time_interval)

        # if counter_aggregation_mode == 'sum' and influx_query_parsing.is_sum_group_by_time(parsed_query):
        if influx_query_parsing.is_sum_group_by_time(parsed_query):
            parsed_query = influx_query_modification.change_sum_group_by_time_factor(parsed_query, '1/' + str(my_factor))

        query = influx_query_parsing.stringify_sqlparsed(parsed_query)
        logging.info('Reworked query (limit nb points): ' + query)

        return query

    return None


@statsd.timed('timer_update_query_to_limit_nb_points_for_query', use_ms=True)
def update_query_to_limit_nb_points_for_query(backend_host, backend_port, user, password,
                                              from_parts, query, parsed_query,
                                              aggregation_properties, max_nb_points_per_query):

    schema = from_parts['schema']

    # counter_aggregation_mode = get_counter_aggregation_mode(from_parts, aggregation_properties)
    # if counter_aggregation_mode is None:
    #     counter_aggregation_mode = 'mean'

    expected_nb_points_per_query = get_expected_nb_points_for_query(backend_host, backend_port, user, password, schema,
                                                                    query, parsed_query)
    if expected_nb_points_per_query is None:
        return None
    if expected_nb_points_per_query['nb_points'] > max_nb_points_per_query:
        logging.info('Expected nb of points per query ' + str(
            expected_nb_points_per_query['nb_points']) + ' is bigger than max allowed one (' + str(
            max_nb_points_per_query) + ')')

        my_factor = expected_nb_points_per_query['nb_points'] // max_nb_points_per_query
        split_group_by_time_interval = influx_date_manipulation.split_influx_time(
            expected_nb_points_per_query['group_by_time_interval'])
        adjusted_group_by_time_value = int(math.ceil(my_factor * split_group_by_time_interval['number']))
        new_group_by_time_interval = str(adjusted_group_by_time_value) + split_group_by_time_interval['unit']

        parsed_query = influx_query_modification.change_group_by_time_interval(parsed_query, new_group_by_time_interval)

        # if counter_aggregation_mode == 'sum' and influx_query_parsing.is_sum_group_by_time(parsed_query):
        if  influx_query_parsing.is_sum_group_by_time(parsed_query):
            parsed_query = influx_query_modification.change_sum_group_by_time_factor(parsed_query,
                                                                                     '1/' + str(my_factor))

        query = influx_query_parsing.stringify_sqlparsed(parsed_query)
        logging.info('Reworked query (limit nb points per query): ' + query)

        return query

    return None


# ------------------------------------------------------------------------
# PRIVATE

def get_counter_aggregation_mode(from_part, aggregation_properties):
    schema = from_part['schema']
    measurement = from_part['measurement']

    props = None
    if schema in aggregation_properties:
        props = aggregation_properties[schema]
    elif 'default' in aggregation_properties:
        props = aggregation_properties['default']

    if props is None:
        logging.info('no known counter aggregation mode for schema ' + schema)
        return None

    for rule in aggregation_properties[schema]:
        if re.match(rule['regexp'], measurement):
            return rule['function']


@statsd.timed('timer_get_right_rp_for_query', use_ms=True)
def get_right_rp_for_query(schema, query, parsed_query, known_retention_policies, override_explicit_rp=False):
    output = {}
    chosen_rp = None
    chosen_group_by_time_interval = None
    is_query_sum_group_by_time = False

    from_parts = influx_query_parsing.extract_measurement_from_query(schema, parsed_query)
    if from_parts is None:
        logging.error('Could not extract measurement from query')
        return None
    if schema is None and from_parts['schema'] is None:
        # pass-through towards InfluxDB
        logging.warning('Schema not specified in query nor as a URL param')
        return None
    if schema != from_parts['schema']:
        # schema in query overrides URL param
        schema = from_parts['schema']

    if from_parts['rp'] is not None and override_explicit_rp is False:
        logging.info('RP ' + from_parts['rp'] + ' set in query, skipping')
        return None

    if schema not in known_retention_policies:
        logging.info('no known RP for schema ' + schema)
        return None

    time_bounds = influx_query_parsing.extract_time_window_bounds(query)
    group_by_time_interval = influx_query_parsing.extract_time_interval_group_by(parsed_query)
    if group_by_time_interval is not None:
        is_query_sum_group_by_time = influx_query_parsing.is_sum_group_by_time(parsed_query)

    if time_bounds['from'] is None:
        logging.info('no lower time boundary in query, cannot select automatically RP')
        return None
    logging.debug('lower time bound is ' + time_bounds['from'].strftime("%Y-%m-%d %H:%M:%S"))

    rp = None
    if from_parts['rp'] is not None:
        logging.debug('RP defined explicitly in query: ' + from_parts['rp'])
        rp = next((rp for rp in known_retention_policies[schema] if rp['name'] == from_parts['rp']), None)
        if rp is None:
            logging.warning('explicit RP ' + from_parts['rp'] + ' is unknown')
            return None
    else:
        logging.debug('no explicit RP in query')
        rp = influx_rp_conf_access.get_default_rp_for_schema_from_conf(schema, known_retention_policies)
        if rp is None:
            logging.error('missing default RP in known RP list')
            return None

    max_datetime = influx_date_manipulation.datetime_max_for_influx_rp(rp['duration'])
    logging.debug('Selected RP (' + rp['name'] + ') max datetime is ' + max_datetime.strftime("%Y-%m-%d %H:%M:%S"))
    if is_rp_good_for_our_interval(max_datetime, time_bounds['from']):
        logging.info('Selected RP (' + rp['name'] + ') is already pretty good')
    else:
        for rp in known_retention_policies[schema]:
            max_datetime = influx_date_manipulation.datetime_max_for_influx_rp(rp['duration'])
            logging.debug('RP ' + rp['name'] + ' max datetime is ' + max_datetime.strftime("%Y-%m-%d %H:%M:%S"))
            if is_rp_good_for_our_interval(max_datetime, time_bounds['from']):
                logging.info('RP ' + rp['name'] + ' is selected')
                chosen_rp = rp['name']
                if group_by_time_interval is not None and 'interval' in rp:
                    chosen_group_by_time_interval = get_new_group_by_time_interval_according_to_rp(
                        group_by_time_interval, from_parts['measurement'], rp)
                break

    if chosen_rp is not None:
        output['rp'] = chosen_rp

    if chosen_group_by_time_interval is not None:
        output['group_by_time_interval'] = chosen_group_by_time_interval
        if is_query_sum_group_by_time:
            output['sum_group_by_time_interval_factor'] = get_sum_group_by_time_interval_factor(group_by_time_interval,
                                                                                                chosen_group_by_time_interval)

    if not output:
        return None

    return output


def is_rp_good_for_our_interval(rp_max_datetime, from_time_bound):
    # NB: We allow ourselves a small margin for the edge case when both date are equal, but we get a few
    #     Another way would have been to .replace(microsecond=0) on each datetime
    margin = timedelta(seconds=1)
    return rp_max_datetime - timedelta(seconds=1) <= from_time_bound


def get_new_group_by_time_interval_according_to_rp(current_group_by_time_interval, measurement, new_rp):
    # TODO: we might want to be able to (re)define it on a per-measurement basis
    current = influx_date_manipulation.influx_interval_to_timedelta(current_group_by_time_interval)
    new = influx_date_manipulation.influx_interval_to_timedelta(new_rp['interval'])
    if current < new:
        logging.debug('Selected RP (' + new_rp['name'] + ') has lower precision interval (' + new_rp['interval'] +
                      ') than what query asks for (' + current_group_by_time_interval + '), changing to: ' +
                      new_rp['interval'])
        return new_rp['interval']
    else:
        logging.debug('Selected RP (' + new_rp['name'] + ') has higher or equal precision interval (' +
                      new_rp['interval'] + ') than what query asks for (' + current_group_by_time_interval +
                      '), no change')


def get_sum_group_by_time_interval_factor(current_group_by_time_interval, new_group_by_time_interval):
    current = influx_date_manipulation.influx_interval_to_nanoseconds(current_group_by_time_interval)
    new = influx_date_manipulation.influx_interval_to_nanoseconds(new_group_by_time_interval)

    if current < new:
        ratio_str = str(current) + ' / ' + str(new)
    else:
        ratio_str = str(new) + ' / ' + str(current)

    logging.debug('Ratio to keep unit of rate (query of type SUM GROUP BY time()) is: ' + ratio_str)
    return ratio_str


# ------------------------------------------------------------------------
# NB POINTS

@statsd.timed('timer_get_expected_nb_points_per_series', use_ms=True)
def get_expected_nb_points_per_series(query, parsed_query):
    query_window_timedelta = influx_query_parsing.get_query_time_window(query)
    if query_window_timedelta is None:
        return None

    group_by_time_interval = influx_query_parsing.extract_time_interval_group_by(parsed_query)

    group_by_time_ns = influx_date_manipulation.influx_interval_to_nanoseconds(group_by_time_interval)
    query_window_ns = influx_date_manipulation.timedelta_to_ns(query_window_timedelta)

    return {
        'group_by_time_interval': group_by_time_interval,
        'nb_points': int(query_window_ns // group_by_time_ns)
    }


@statsd.timed('timer_get_expected_nb_points_for_query', use_ms=True)
def get_expected_nb_points_for_query(backend_host, backend_port, user, password, schema, query, parsed_query):
    per_series = get_expected_nb_points_per_series(query, parsed_query)
    if per_series is None:
        return None
    nb_series = influx_querying_spe.get_number_series_for_query(backend_host, backend_port, user, password, schema,
                                                                query)
    return {
        'group_by_time_interval': per_series['group_by_time_interval'],
        'nb_points': per_series['nb_points'] * nb_series
    }
