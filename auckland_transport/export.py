"""
This file exports the data needed for the dashboard from the database to csv files
"""

import sqlalchemy
import os
import pandas as pd
import database_actions
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def export_data(engine: sqlalchemy.engine.base.Engine, folder: str):
    """
        Exports the data needed for the dashboard to csv files
    Args:
        engine (sqlalchemy.engine.base.Engine): The database engine where the data is stored
        folder (str): The folder where the csv files will be saved

    Returns:
        None
    """
    schema = 'auckland_transport_dashboard'
    tables = ['state_over_time',
              'overall_state',
              'delay_by_time',
              'worst_routes',
              'worst_vehicles'
             ]

    with engine.connect() as conn:
        for t in tables:
            query = f"SELECT * FROM {schema}.{t}"
            file = f"{folder}/{t}.csv"
            df = pd.read_sql(query, conn)
            logger.info(f"Exporting {t} to {file}")
            df.to_csv(file, index=False)

def main():
    connection_string = os.getenv('DATABASE_URL')
    folder = os.getenv('AUCKLAND_TRANSPORT_FOLDER')
    db = database_actions.DatabaseActions(connection_string)
    engine = db.get_engine()
    export_data(engine, folder)


if __name__ == "__main__":
    main()