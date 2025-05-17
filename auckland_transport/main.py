import os
from sqlalchemy.orm import Session
import logging
import time

import download_data
import database_actions
import database_base


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def main():
    data = download_data.make_realtime_request()

    connection_string = os.getenv('DATABASE_URL')
    db = database_actions.DatabaseActions(connection_string)
    engine = db.get_engine()

    database_base.Base.metadata.create_all(bind=engine)
    with Session(engine) as session:

        trip_updates = download_data.convert_request_to_trip_update(data)
        logger.info(f'Adding {len(trip_updates)} trip updates to database')

        if len(trip_updates) == 0:
            logger.warning('No trip updates found, skipping')
        session.add_all(trip_updates)
        session.commit()


def loop():
    while True:
        main()
        time.sleep(20*60)


if __name__ == "__main__":
    loop()