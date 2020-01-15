from datadog import statsd

import cleanflux.utils.influx.querying as influx_querying
import cleanflux.utils.influx.query_modification as influx_query_modification


@statsd.timed('timer_get_number_series_for_query', use_ms=True)
def get_number_series_for_query(backend_host, backend_port, user, password, schema, query):
    nb_series_query = influx_query_modification.add_limit(query, 1)
    result_df_dict = influx_querying.pd_query(backend_host, backend_port, user, password, schema, nb_series_query)
    return influx_querying.get_nb_series_in_pd_result(result_df_dict)
