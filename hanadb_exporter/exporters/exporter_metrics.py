"""
SAP HANA database data exporter metrics

:author: xarbulu
:organization: SUSE Linux GmbH
:contact: xarbulu@suse.de

:since: 2019-05-09
"""

import logging
import collections
import json


METRICMODEL = collections.namedtuple(
    'Metric',
    'name description labels value unit type enabled'
)


class Metric(METRICMODEL):
    """
    Class to store the loaded metrics inherited from namedtuple
    """

    # pylint:disable=R0913
    # pylint:disable=W0622
    def __new__(cls, name, description, labels, value, unit, type, enabled=True):
        logging.getLogger(__name__).debug('Parsing new metric: %s', name)
        metric = super(Metric, cls).__new__(
            cls, name, description, labels, value, unit, type, enabled)
        logging.getLogger(__name__).debug('Metric parsed correctly')
        return metric


class Query(object):
    """
    Class to store the query and its metrics
    """

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self.query = None
        self.metrics = []
        self.enabled = True

    def parse(self, query, query_data):
        """
        Parse metrics by query
        """
        self._logger.debug('Parsing new query: %s ...', query[:50])
        self.query = query
        self.metrics = []
        self.enabled = query_data.get('enabled', True)
        self._logger.debug('Query enabled status: %s', self.enabled)
        for metric in query_data['metrics']:
            modeled_data = Metric(**metric)
            self.metrics.append(modeled_data)
        self._logger.debug('Query parsed correctly')

    @classmethod
    def get_model(cls, query, metrics):
        """
        Get metric model data
        """
        modeled_query = cls()
        modeled_query.parse(query, metrics)
        return modeled_query


class ExporterMetrics(object):
    """
    Class to store the metrics data
    """

    def __init__(self, metrics_file):
        self.queries = self.load_metrics(metrics_file)

    @classmethod
    def load_metrics(cls, metrics_file):
        """
        Load metrics file as json
        """
        logger = logging.getLogger(__name__)
        logger.info('Loading metrics from %s', metrics_file)
        queries = []
        with open(metrics_file, 'r') as file_ptr:
            data = json.load(file_ptr)

        try:
            for query, query_data in data.items():
                modeled_query = Query.get_model(query, query_data)
                queries.append(modeled_query)
        except TypeError as err:
            logger.error('Malformed %s file in query %s ...', metrics_file, query[:50])
            logger.error(err)
            raise

        logger.info('Metrics file %s loaded correctly', metrics_file)
        return queries
