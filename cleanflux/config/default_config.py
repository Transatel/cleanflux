DEFAULT_CONFIG = {

    # cleanflux server address
    'host': 'localhost',
    'port': 8888,

    # Connection to the time series database API
    'backend_host': 'localhost',
    'backend_port': 8086,
    'backend_user': None,
    'backend_password': None,

    # Corrective rules
    'rules': [
        'remove_partial_intervals_case_sum_group_by_time',
    ],

    'max_nb_points_per_series': None,
    'max_nb_points_per_query': None,

    # Max values of fields before overflow
    'counter_overflows': {},

    # Aggregation properties, i.e. how we defined aggregation / downsampling
    'aggregation_properties': [],

    # Retention Policies definition, for automatic selection depending on time interval
    'auto_retrieve_retention_policies': True, # enable / disable auto retrieve at startup
    'retention_policies': {}, # overrides

    # Run in foreground?
    'foreground': False,

    # Default PID file location
    'pidfile': '/var/run/cleanflux.pid',

    # Smallest possible system date.
    # This is required for the calculation of the max duration between datetime objects.
    # We use the release day of InfluxDB 0.8 as the epoch by default
    # because it is the first official version supported.
    # You can overwrite it with this parameter, though:
    'epoch': None,
    'configfile': None,
    'c': None,

    'use_syslog': False,
    'syslog_address': None,
    'logfile': '/var/log/cleanflux.log',
    'log_level': 'ERROR',
    'v': 0,
}
