"""
SAP HANA database exporter factory

:author: xarbulu
:organization: SUSE Linux GmbH
:contact: xarbulu@suse.de

:since: 2019-06-13
"""

import logging

from hanadb_exporter.exporters import prometheus_exporter
from hanadb_exporter.exporters import azure_exporter
from hanadb_exporter.exporters import exporter_metrics


class SapHanaExporter(object):
    """
    SAP HANA factory exporter

    Args:
        exporter_type (str): Exporter type. Options: prometheus
    """

    @classmethod
    def create(cls, exporter_type='prometheus', **kwargs):
        """
        Create SAP HANA exporter
        """
        cls._logger = logging.getLogger(__name__)
        metrics_config = exporter_metrics.ExporterMetrics(kwargs.get('metrics_file'))
        if exporter_type == 'prometheus':
            cls._logger.info('prometheus exporter selected')
            collector = prometheus_exporter.SapHanaCollector(
                connector=kwargs.get('hdb_connector'),
                metrics_config=metrics_config
            )
            return collector
        elif exporter_type == 'azure':
            cls._logger.info('azure exporter selected')
        else:
            raise NotImplementedError(
                '{} exporter not implemented'.format(exporter_type))
