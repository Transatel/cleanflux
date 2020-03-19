import logging
import numpy as np
import json
import requests
from pprint import pprint
from influxdb import InfluxDBClient, DataFrameClient
from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError
import influxdb
from datadog import statsd


from cleanflux.utils.influx.date_manipulation import pd_timestamp_to_timestamp
from cleanflux.utils.influx.query_sqlparsing import sqlparse_query, get_cq_schema, get_cq_interval, get_cq_from, get_cq_into, parse_measurement_path


# ------------------------------------------------------------------------
# LIB PATCHING

def robustify_influxdb_client():
    def custom_request(self, url, method='GET', params=None, data=None,
                expected_response_code=200, headers=None):
        """Make a HTTP request to the InfluxDB API.

        :param url: the path of the HTTP request, e.g. write, query, etc.
        :type url: str
        :param method: the HTTP method for the request, defaults to GET
        :type method: str
        :param params: additional parameters for the request, defaults to None
        :type params: dict
        :param data: the data of the request, defaults to None
        :type data: str
        :param expected_response_code: the expected response code of
            the request, defaults to 200
        :type expected_response_code: int
        :param headers: headers to add to the request
        :type headers: dict
        :returns: the response from the request
        :rtype: :class:`requests.Response`
        :raises InfluxDBServerError: if the response code is any server error
            code (5xx)
        :raises InfluxDBClientError: if the response code is not the
            same as `expected_response_code` and is not a server error code
        """
        url = "{0}/{1}".format(self._baseurl, url)

        if headers is None:
            headers = self._headers

        if params is None:
            params = {}

        if isinstance(data, (dict, list)):
            data = json.dumps(data)

        # Try to send the request more than once by default (see #103)
        retry = True
        _try = 0
        while retry:
            try:
                response = self._session.request(
                    method=method,
                    url=url,
                    auth=(self._username, self._password),
                    params=params,
                    data=data,
                    headers=headers,
                    proxies=self._proxies,
                    verify=self._verify_ssl,
                    timeout=self._timeout
                )
                break
            except requests.exceptions.ConnectionError as e:
                self._session = requests.Session()
                _try += 1
                if self._retries != 0:
                    retry = _try < self._retries
            except requests.exceptions.ChunkedEncodingError as e:
                logging.warn("Case of broken HTTP session, retring w/ new session")
                self._session = requests.Session()
                _try += 1
                if self._retries != 0:
                    retry = _try < self._retries
        else:
            raise requests.exceptions.ConnectionError

        if 500 <= response.status_code < 600:
            raise InfluxDBServerError(response.content)
        elif response.status_code == expected_response_code:
            return response
        else:
            raise InfluxDBClientError(response.content, response.status_code)
    setattr(influxdb.InfluxDBClient, 'request', custom_request)


# ------------------------------------------------------------------------
# NUMPY DATA ENCODER

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)


# ------------------------------------------------------------------------
# QUERYING: pandas FORMAT

@statsd.timed('timer_pd_query_influxdb', use_ms=True)
def pd_query(backend_host, backend_port, user, password, schema, query):
    pd_influx_client = DataFrameClient(backend_host, backend_port, user, password, schema)
    result_df_dict = pd_influx_client.query(query)  # returns a dict, "<measurement>" => DataFrame
    return result_df_dict


@statsd.timed('timer_rp_auto_detect', use_ms=True)
def get_rp_list(backend_host, backend_port, user, password, schema_list=[]):
    # influx_client = InfluxDBClient(backend_host, backend_port, user, password)

    pd_influx_client = DataFrameClient(backend_host, backend_port, user, password)

    if not schema_list:
        schema_list_raw = pd_influx_client.query('SHOW DATABASES')
        schema_list = [ e['name'] for e in list(schema_list_raw.get_points(measurement='databases'))]

    cq_list_raw = pd_influx_client.query('SHOW CONTINUOUS QUERIES')
    rp_dict = {}
    for schema in schema_list:
        result_df_dict = pd_influx_client.query('SHOW RETENTION POLICIES ON "' + schema + '"')
        rp_list = list(result_df_dict.get_points(measurement='results'))
        cq_list = list(cq_list_raw.get_points(measurement=schema))

        # enrich RPs whith intervals gotten from CQs
        cq_into_rp_set = set()
        for cq in cq_list:
            parsed_cq = sqlparse_query(cq['query'])
            from_m = parse_measurement_path(schema, get_cq_from(parsed_cq))
            into = parse_measurement_path(schema, get_cq_into(parsed_cq))
            if into['measurement'] != ':MEASUREMENT' \
               and into['measurement'] != from_m['measurement']:
                # NB: if insertion in another measurement, skip
                continue
            into_rp = into['rp']
            cq_into_rp_set.add(from_m['rp'])
            cq_into_rp_set.add(into_rp)
            rp_conf_raw = next([rp, i] for i, rp in enumerate(rp_list) if rp['name'] == into_rp)
            if not rp_conf_raw:
                continue
            rp_conf, rp_conf_i = rp_conf_raw
            if 'interval' in rp_conf:
                # NB: we assumme all CQ for all measurements with a same INTO RP use the same GROUP BY time interval
                # this is a strong and limitative assumption
                continue
            rp_list[rp_conf_i]['interval'] = get_cq_interval(parsed_cq)

        # remove RPs from rp_dict that don't match a CQ
        active_rp_list = []
        for i, rp in enumerate(rp_list):
            if rp['name'] in cq_into_rp_set:
               active_rp_list.append(rp)
        if active_rp_list:
            rp_dict[schema] = active_rp_list
    return rp_dict


def get_nb_series_in_pd_result(resultset_list):
    nb_series = 0
    for resultset in resultset_list:
        nb_series += len(resultset)
    return nb_series


def pd_result_to_influx_result(resultset_list, precision):
    output_dict = {
        'results': []
    }

    for resultset in resultset_list:
        query_dict = {
            'series': []
        }
        for series in resultset:
            tags = {}
            if isinstance(series, tuple):
                measurement = series[0]
                for raw_tag in series[1]:
                    tags[raw_tag[0]] = raw_tag[1]
            else:
                measurement = series

            df = resultset[series]
            columns = df.columns.values.tolist()
            all_columns = ['time'] + columns
            series_dict = {
                'name': measurement,
                'columns': all_columns,
                'values': []
            }
            if tags:
                series_dict['tags'] = tags
            for index, row in df.iterrows():
                # TODO: should change precision according to param epoch (ns, ms ...)
                row_values = [pd_timestamp_to_timestamp(index, precision)]
                for column in columns:
                    value = row[column]
                    if np.isnan(value):
                        value = None
                    row_values.append(value)
                    # logging.debug("dump {0} -> {1}".format(index.value, row[column]))
                    series_dict['values'].append(row_values)
            query_dict['series'].append(series_dict)
        output_dict['results'].append(query_dict)

    return json.dumps(output_dict, cls=NpEncoder)
