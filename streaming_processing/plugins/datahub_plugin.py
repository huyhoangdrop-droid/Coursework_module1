from datahub_airflow_plugin.datahub_listener import DataHubListener
from datahub_airflow_plugin._config import DatahubLineageConfig
from airflow.plugins_manager import AirflowPlugin

_config = DatahubLineageConfig.parse_obj({
    "enabled": True, # This setting enables the DataHub Airflow plugin, allowing it to capture metadata and lineage information from our Airflow workflows and send it to DataHub for tracking and visualization.
    "datahub_conn_id": "datahub_rest_default",
    "cluster": "prod", # This setting specifies the cluster name that will be associated with the metadata captured by the plugin. It helps to differentiate between different environments (e.g., dev, staging, prod) in DataHub.
    "platform_instance": None,
    "capture_ownership_info": True,
    "capture_ownership_as_group": False,
    "capture_tags_info": True,
    "materialize_iolets": True, # This setting enables the materialization of I/Olets, which are a way to represent the inputs and outputs of Airflow tasks in DataHub.
    "capture_airflow_assets": True, # This setting enables the capture of Airflow assets (e.g., DAGs, tasks) in DataHub, allowing us to track and visualize our Airflow workflows within the DataHub platform.
    "capture_executions": True, # This setting enables the capture of execution information for Airflow tasks, allowing us to track when tasks are executed, their status, and other relevant metadata in DataHub.
    "datajob_url_link": "taskinstance",
    "enable_extractors": True, # Extractors are responsible for parsing SQL queries. These queries can be found in various Airflow operators (e.g., PostgresOperator, SnowflakeOperator, etc.).
    "patch_sql_parser": False,
    "patch_snowflake_schema": False,
    "extract_athena_operator": False,
    "extract_bigquery_insert_job_operator": False,
    "extract_teradata_operator": False,
    "render_templates": False,
    "enable_multi_statement_sql_parsing": False,
    "enable_datajob_lineage": True, # This setting enables the capture of lineage information at the data job level, allowing us to see how different tasks within a data job are connected and how data flows through the entire job.
    "dag_filter_pattern": {"allow": [".*"], "deny": []},
    "log_level": None,
    "debug_emitter": False,
    "disable_openlineage_plugin": False,
})


class DataHubPlugin(AirflowPlugin):
    name = "datahub_plugin"
    listeners = [DataHubListener(_config)]
