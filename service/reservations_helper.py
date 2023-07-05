import json
import os
from datetime import datetime, timedelta

from configurations.reservations_configurations import ReservationsConfigurations
from logging import Logger

from service.database_helper import DatabaseHelper


class ReservationsHelper(ReservationsConfigurations):
    def __init__(self, logger: Logger):
        super().__init__()
        self.logger = logger
        self.database_helper = DatabaseHelper()
        self.reservations_file_name = self.config["reservations_helper"]["reservations_file_name"]

    def database_setup(self):
        conn, cursor = self.database_helper.create_connection_to_db()
        self.database_helper.verify_table(conn, cursor)
        return conn, cursor

    def verify_reservations_file(self) -> str:
        if not os.path.isfile(self.reservations_file_name):
            raise ValueError("reservations file wasn't found")
        return self.reservations_file_name

    def update_database_caller(self, conn, cursor, reservations_file_name):
        self.database_helper.update_database(conn, cursor, reservations_file_name)

    def get_hotel_rooms_count(self):
        with open('hotel_information.json', 'r') as file:
            hotel_rooms_count = json.load(file)

        return hotel_rooms_count

    def fetch_reservation_from_database_caller(self, reservation_id: str):
        try:
            reservation = self.database_helper.fetch_reservation_from_database(reservation_id)
        except Exception as ex:
            extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
            self.logger.error(f"an error occurred while trying to fetch reservation from database",
                              extra={"extra": extra_msg})
            # placeholder for retry mechanism
            return None
        if not reservation:
            extra_msg = f"reservation_id is : {reservation_id}"
            self.logger.error("no results returned from database", extra={"extra": extra_msg})
            return None

        return reservation

    def parse_reservation(self, reservation):
        response_keys = ["reservation_id", "room_id", "hotel_id", "guest_name", "email", "arrival_date", "nights", "country"]
        reservation_data = {}

        for key, value in zip(response_keys, reservation):
            reservation_data[key] = value

        return reservation_data

    def fetch_data_from_database_caller(self, reservation_id: str):
        try:
            reservation = self.database_helper.fetch_reservation_from_database(reservation_id)
        except Exception as ex:
            extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
            self.logger.error(f"an error occurred while trying to fetch reservation data from database",
                              extra={"extra": extra_msg})
            # placeholder for retry mechanism
            return None
        if not reservation:
            extra_msg = f"reservation_id is : {reservation_id}"
            self.logger.error("no results returned from database", extra={"extra": extra_msg})
            return None

        return reservation

    def fetch_relevant_rooms_from_database_caller(self, arrival_date_str, nights, room_id):
        arrival_date = datetime.strptime(arrival_date_str, '%Y-%m-%d %H:%M:%S.%f %z')
        end_date = arrival_date + timedelta(days=int(nights))
        end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S.%f %z')
        return self.database_helper.fetch_relevant_reservations_from_database(arrival_date_str, end_date_str, room_id)

    def create_hotel_room_availability(self, hotel_room_availability, room_id, relevant_reservations):
        hotel_room_availability = hotel_room_availability["13"]["inventory"]
        rooms_to_remove = []
        for key in hotel_room_availability.keys():
            if key <= room_id:
                rooms_to_remove.append(key)

        for key in rooms_to_remove:
            hotel_room_availability.pop(key)

        for reservation in relevant_reservations:
            reservation_room_id = reservation[1]
            hotel_room_availability[reservation_room_id] = hotel_room_availability[reservation_room_id] - 1

        return hotel_room_availability