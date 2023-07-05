import csv
import sqlite3

from configurations.reservations_configurations import ReservationsConfigurations


class DatabaseHelper(ReservationsConfigurations):
    def __init__(self):
        super().__init__()
        self.database_path = self.config["database_helper"]["reservations_db_path"]
        self.table_name = self.config['database_helper']['table_name']

    def create_connection_to_db(self):
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        return conn, cursor

    def verify_table(self, conn, cursor):
        # Check if table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table_name}';")
        if not cursor.fetchone():
            cursor.execute((f'''
            CREATE TABLE {self.table_name}(
                reservation_id TEXT PRIMARY KEY,
                room_id TEXT,
                hotel_id TEXT,
                guest_name TEXT,
                email TEXT,
                arrival_date TEXT,
                nights TEXT,
                country TEXT)'''))
            conn.commit()

    def update_database(self, conn, cursor, reservations_file_name):
        with open(reservations_file_name, "r") as reservations_file:
            csv_data = csv.reader(reservations_file)
            next(csv_data)

            for row in csv_data:
                insert_query = f"INSERT OR IGNORE INTO {self.table_name} (reservation_id, room_id, hotel_id, guest_name, email, arrival_date, nights, country) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                cursor.execute(insert_query, tuple(row))
            conn.commit()

    def fetch_reservation_from_database(self, reservation_id: str):
        conn, cursor = self.create_connection_to_db()
        fetch_query = f"SELECT * FROM {self.table_name} WHERE reservation_id = '{reservation_id}'"
        cursor.execute(fetch_query)
        reservation = cursor.fetchall()[0]
        conn.close()
        return reservation

    def fetch_relevant_reservations_from_database(self, arrival_date_str, end_date_str, room_id):
        conn, cursor = self.create_connection_to_db()
        fetch_query = f"SELECT * FROM {self.table_name} WHERE room_id > '{room_id}' AND arrival_date >= '{arrival_date_str}' AND arrival_date <= '{end_date_str}'"
        cursor.execute(fetch_query)
        relevant_reservations = cursor.fetchall()
        conn.close()
        return relevant_reservations