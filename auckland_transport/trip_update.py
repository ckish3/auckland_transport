import datetime
from database_base import Base
import database_actions
from sqlalchemy import String, Date, BigInteger
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class TripUpdate(Base):
    __tablename__ = "trip_update"
    __table_args__ = {"schema": database_actions.DatabaseActions.raw_schema_name}

    id: Mapped[str] = mapped_column(primary_key=True)
    trip_id: Mapped[str] = mapped_column(String(45))
    route_id: Mapped[str] = mapped_column(String(10))
    direction_id: Mapped[int]
    start_time: Mapped[str] = mapped_column(String(12))
    start_date: Mapped[str] = mapped_column(String(8))
    stop_id: Mapped[str] = mapped_column(String(13))
    stop_time: Mapped[int] = mapped_column(BigInteger)
    stop_delay: Mapped[int]
    stop_uncertainty: Mapped[int]
    stop_sequence: Mapped[int]
    vehicle_id: Mapped[str] = mapped_column(String(10))
    vehicle_license_plate: Mapped[str] = mapped_column(String(11))
    trip_delay: Mapped[int]
    timestamp: Mapped[int] = mapped_column(BigInteger)



    def __repr__(self) -> str:
        return f"TripUpdate(id={self.id}, )"

