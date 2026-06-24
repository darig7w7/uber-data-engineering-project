from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

PROJECT_ID = 'uber-propject'
DATASET = 'uber_data_engineering'


@data_exporter
def export_data_to_big_query(data, **kwargs) -> None:
    """
    Sube cada tabla del star schema (dimensiones + fact_table) a BigQuery.
    `data` es el dict que devolvió el transformer.
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    for table_name, rows in data.items():
        table_id = f'{PROJECT_ID}.{DATASET}.{table_name}'
        BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            DataFrame(rows),
            table_id,
            if_exists='replace',  # reemplaza la tabla si ya existe
        )
