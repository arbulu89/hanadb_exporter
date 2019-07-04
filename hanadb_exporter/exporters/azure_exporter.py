"""
SAP HANA database azure data exporter

:author: xarbulu
:organization: SUSE Linux GmbH
:contact: xarbulu@suse.de

:since: 2019-07-03
"""

import logging
import datetime
import hashlib
import hmac
import base64
import pprint
import requests


class SapHanaCollector(object):
    """
    SAP HANA database data exporter

    Args:
        connector (hdb_connector): SAP HANA database connector
        metrics_config (ExporterMetrics): Metrics data loaded from metrics file
        ms_config (dict): Data with MS configuration (workspace_id and shared_key)
    """

    TIME_FORMAT_HANA = '%Y-%m-%d %H:%M:%S.%'
    TIME_FORMAT_LOG_ANALYTICS = '%a, %d %b %Y %H:%M:%S GMT'
    TIMESTAMP_FIELD = 'UTC_TIMESTAMP'
    LOG_TYPE = 'SapHana_Infra'
    ANALYTICS_URI = 'https://{workspace}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01'

    def __init__(self, connector, metrics_config, ms_config_data):
        self._logger = logging.getLogger(__name__)
        self._hdb_connector = connector
        self._metrics_config = metrics_config
        self._ms_config_data = ms_config_data
        self._uri = self._ms_config_data.get('uri') or \
            self.ANALYTICS_URI.format(self._ms_config_data['workspace_id'])

    # TODO: Maybe, this method should go in shaptools QueryResults (add formatted paramter to query)
    # It should return a list of dictionaries where {'column1': 'data1', 'column2': 'data2', ...}
    def _format_query_result(self, query_result):
        """
        Format query results to match column names with their values for each row
        Returns nested list, containing tuples (column_name, value)

        Args:
            query_result (obj): QueryResult object
        """
        # TODO: append timestamp to the each entry
        formatted_query_result = []
        query_columns = [meta[0] for meta in query_result.metadata]

        # Add timestap to record if it was not queried
        timestamp = None
        if self.TIMESTAMP_FIELD not in query_columns:
            # TODO: adapt with HANA db time offset
            timestamp = datetime.datetime.utcnow().strftime(self.TIME_FORMAT_HANA)

        for record in query_result.records:
            record_dict = {self.TIMESTAMP_FIELD: timestamp} # Initialize with timestamp just in case
            for index, record_item in enumerate(record):
                if query_columns[index].startswith("_"): # remove internal fields
                    continue
                record_dict[query_columns[index]] = record_item

            formatted_query_result.append(record_dict)
        return formatted_query_result

    def __create_headers(self, data):
        """
        Create headers to ingest data

        Args:
            data (dict): Data to ingest
        """
        timestamp = datetime.datetime.utcnow().strftime(self.TIME_FORMAT_LOG_ANALYTICS)
        string_hash = ('POST'
                       '{data_length}'
                       'application/json'
                       'x-ms-date:{timestamp}'
                       '/api/logs').format(data_length=len(data), timestamp=timestamp)
        bytes_hash = bytes(string_hash, encoding='utf-8')
        decoded_key = base64.b64decode(self._ms_config_data['shared_key'])
        encoded_hash = base64.b64encode(
            hmac.new(
                decoded_key,
                bytes_hash,
                digestmod=hashlib.sha256).digest())
        string_hash = encoded_hash.decode('utf-8')
        shared_key = 'SharedKey {}:{}'.format(self._ms_config_data['workspace_id'], string_hash)

        headers = {
            'content-type': 'application/json',
            'Authorization': shared_key,
            'Log-Type': self.LOG_TYPE,
            'x-ms-data': timestamp
        }
        return headers

    # pylint:disable=R1710
    def ingest(self, data):
        """
        Ingest gathered queries data in Azure Log analytics server

        Args:
            data (dict): Data to ingest
        """
        headers = self.__create_headers(data)
        response = requests.post(self._uri, headers=headers, json=data)
        if response.status_code == requests.codes.ok:
            self._logger.debug(pprint.pprint(response.content, indent=4))
            return response.content
        else:
            self._logger.error(response.content)
            response.raise_for_status()

    def collect(self):
        """
        Collect data from database and send to the Analytic logs server
        """

        data_list = []
        for query in self._metrics_config.queries:
            #  execute each query once (only if enabled)
            if query.enabled:
                # TODO: manage query error in an exception
                query_result = self._hdb_connector.query(query.query)
                formatted_query_result = self._format_query_result(query_result)
                data_list.append(formatted_query_result)
            else:
                self._logger.info('Query %s is disabled', query.query)
        return data_list
