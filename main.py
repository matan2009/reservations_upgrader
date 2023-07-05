import uvicorn as uvicorn
from fastapi import FastAPI
from monitoring.logger import Logger
from service.reservations_helper import ReservationsHelper
from type.request_dto import RequestDTO

app = FastAPI()


@app.get('/get_reservation')
def get_reservation(request_dto: RequestDTO):
    # placeholder for input validation (fields are in the correct format for example)
    reservation_id = request_dto.reservation_id
    return reservations.get_reservation_info(reservation_id)


@app.get('/get_availability')
def get_availability(request_dto: RequestDTO):
    # placeholder for input validation (fields are in the correct format for example)
    reservation_id = request_dto.reservation_id
    return reservations.get_reservation_availability(reservation_id)


class Reservations:
    def __init__(self, logger: Logger):
        self.logger = logger.logger
        self.reservations_helper = ReservationsHelper(self.logger)

    def set_samples_in_database(self, conn, cursor):
        try:
            reservations_file_name = self.reservations_helper.verify_reservations_file()
        except ValueError as ex:
            extra_msg = f"the exception is: {str(ex)}, the exception_type is: {type(ex).__name__}"
            self.logger.error("reservations file is not exist", extra={"extra": extra_msg})
            conn.close()
            return None

        self.reservations_helper.update_database_caller(conn, cursor, reservations_file_name)
        conn.close()

    def get_reservation_info(self, reservation_id: str):
        extra_msg = f"reservation_id is: {reservation_id}"
        self.logger.info("got a request to get reservation info", extra={"extra": extra_msg})
        reservation = self.reservations_helper.fetch_reservation_from_database_caller(reservation_id)
        if not reservation:
            return {}
        return self.reservations_helper.parse_reservation(reservation)

    def get_reservation_availability(self, reservation_id: str):
        extra_msg = f"reservation_id is: {reservation_id}"
        self.logger.info("got a request to get reservation availability", extra={"extra": extra_msg})
        reservation = self.reservations_helper.fetch_data_from_database_caller(reservation_id)
        if not reservation:
            return {}
        arrival_date, nights, room_id = reservation[5], reservation[6], reservation[1]
        relevant_reservations = self.reservations_helper.fetch_relevant_rooms_from_database_caller(arrival_date, nights,
                                                                                                   room_id)
        hotel_room_availability = self.reservations_helper.create_hotel_room_availability(hotel_rooms_count, room_id,
                                                                                          relevant_reservations)

        return hotel_room_availability


if __name__ == '__main__':
    logger = Logger()
    reservations = Reservations(logger)
    conn, cursor = reservations.reservations_helper.database_setup()
    reservations.set_samples_in_database(conn, cursor)
    hotel_rooms_count = reservations.reservations_helper.get_hotel_rooms_count()
    uvicorn.run(app, host="0.0.0.0", port=8000)
