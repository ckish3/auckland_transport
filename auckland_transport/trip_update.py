import datetime
from database_base import Base
import database_actions as database_actions
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
        return f"""TripUpdate(id={self.id}, trip_id={self.trip_id}, route_id={self.route_id}, 
direction_id={self.direction_id}, start_time={self.start_time}, start_date={self.start_date}, stop_id={self.stop_id}, 
stop_time={self.stop_time}, stop_delay={self.stop_delay}, stop_uncertainty={self.stop_uncertainty}, 
stop_sequence={self.stop_sequence}, vehicle_id={self.vehicle_id}, vehicle_license_plate={self.vehicle_license_plate}, 
trip_delay={self.trip_delay}, timestamp={self.timestamp})"""

    def attribute_list(self) -> list:
        """
        Return a list of all non-callable, non-internal attributes of the instance.

        Used (primarily) for the equality check.
        """
        result = [a for a in dir(self) if not a.startswith('_') and not callable(getattr(self, a)) \
                  and not a.startswith('id')]

        return result

    def __eq__(self, other: object) -> bool:
        """
        Overloads the '==' operator for TripUpdate objects, i.e. compares two TripUpdate objects for equality.
        Two TripUpdate objects are equal if all of their attributes (non-callable and non-internal) are equal.
        Args:
            other (TripUpdate): The other TripUpdate object to compare to.

        Returns:
            bool: True if the two TripUpdate objects are equal, False otherwise.
        """

        if isinstance(other, TripUpdate):
            is_match = True
            for attr in self.attribute_list():
                print(attr, getattr(self, attr), getattr(other, attr))
                if getattr(self, attr) != getattr(other, attr):
                    is_match = False
                    break
            return is_match
        else:
            return NotImplemented
