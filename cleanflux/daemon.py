# -*- coding: utf-8 -*-

import logging
import logging.handlers
import sys
import traceback
import StringIO
from socket import gethostname
from datadog import initialize as initialize_statsd

from cleanflux.proxy import server
from cleanflux.proxy import request_handler
from cleanflux.utils.influx.querying import robustify_httplib_response_read, robustify_influxdb_client


def add_custom_print_exception():
    old_print_exception = traceback.print_exception
    def custom_print_exception(etype, value, tb, limit=None, file=None):
        tb_output = StringIO.StringIO()
        traceback.print_tb(tb, limit, tb_output)
        logger = logging.getLogger('')
        logger.error(tb_output.getvalue())
        tb_output.close()
        old_print_exception(etype, value, tb, limit=None, file=None)
    traceback.print_exception = custom_print_exception


class HttpDaemon(object):
    def __init__(self,
                 config,
                 cleanflux,
                 handler_class=request_handler.ProxyRequestHandler,
                 server_class=server.ThreadingHTTPServer,
                 protocol="HTTP/1.1"
                 ):
        self.config = config
        self.cleanflux = cleanflux
        self.handler_class = handler_class
        self.server_class = server_class
        self.protocol = protocol

    def show_startup_message(self):
        logging.info("Serving cleanflux on {}:{}...".format(self.config.host, self.config.port))
        logging.info("Backend host (connection to Time Series Database) at {}:{}...".format(self.config.backend_host,
                                                                                            self.config.backend_port))
        logging.info("The following rules are enabled:")
        for rule in self.config.rules:
            logging.info("* {}".format(rule))
        if self.config.foreground:
            logging.info("Starting in foreground...")

    def configure_logging(self):
        logging.getLogger('').handlers = []
        logging_config = {
            "format": '%(asctime)s [%(levelname)s] %(message)s'
        }
        if not self.config.use_syslog:
            if self.config.foreground:
                logging_config["stream"] = sys.stdout
            else:
                logging_config["filename"] = self.config.logfile
                logging_config["filemode"] = "a"

        logging.basicConfig(**logging_config)

        if self.config.use_syslog:
            syslog_address = "/dev/log"
            if self.config.syslog_address is not None:
                syslog_address = self.config.syslog_address
            handler = logging.handlers.SysLogHandler(address=syslog_address)
            # formatter = logging.Formatter("%(module)s-%(processName)s[%(process)d]: %(name)s: %(message)s")
            # https://docs.python.org/2/howto/logging-cookbook.html
            formatter = logging.Formatter("cleanflux[%(process)d]: [%(levelname)s] %(message)s")
            handler.setFormatter(formatter)
            logging.getLogger().addHandler(handler)

        # log exceptions
        add_custom_print_exception()

    def configure_statsd(self):
        statsd_host = None
        if hasattr(self.config, 'statsd_host'):
            statsd_host = self.config.statsd_host
        statsd_port = None
        if hasattr(self.config, 'statsd_port'):
            statsd_port = self.config.statsd_port
        if statsd_host and statsd_port:
            hostname = gethostname()
            initialize_statsd(statsd_host=statsd_host, statsd_port=statsd_port,
                              statsd_constant_tags=['host:' + hostname])

    def run(self):
        self.configure_logging()
        self.configure_statsd()
        # robustify_httplib_response_read()
        robustify_influxdb_client()
        self.show_startup_message()
        logging.info('Daemon is starting')
        server_address = (self.config.host, self.config.port)
        backend_address = (self.config.backend_host, self.config.backend_port)

        # Subclassing BaseHTTPServer requires to pass args and kwargs to the parent class
        # but still there probably is a smarter way to do this...
        self.handler_class.protocol_version = self.protocol
        self.handler_class.cleanflux = self.cleanflux
        self.handler_class.backend_address = backend_address

        httpd = self.server_class(server_address, self.handler_class)
        self.serve_forever(httpd)

    @staticmethod
    def serve_forever(httpd):
        logging.info("Ready to handle requests.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            logging.info('^C received, shutting down.')
            httpd.socket.close()
