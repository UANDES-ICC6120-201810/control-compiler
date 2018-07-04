import mysql.connector
import requests
from time import sleep

API_HOST = 'http://proyectozapo.herokuapp.com/api/v1'
API_COMPILER_ENDPOINT = '{0}/vehicle_event'.format(API_HOST)
CLIENT_SECRET = 'Bearer eyJhbGciOiJIUzI1NiJ9.eyJhY2Nlc3NfcG9pbnRfaWQiOjEwLCJleHAiOjE1Mjg5MjU5NDV9.wqQ73Z8ts0sXo10NcUb7xzN3CUD1claDk-2aZ2ofheU'

CONN_PARAMS = {
  'user': 'ALPR',
  'password': 'PASSALPR',
  'host': 'docker-db',
  'database': 'control_point'
}

def connect_to_db(conn_params):
    while True:
        try:
            print "[Info] Connecting to db"
            connection = mysql.connector.connect(buffered=True, **conn_params)
            print "[Info] Connection successful!"
            return connection
        except mysql.connector.errors.InterfaceError:
            print "[Error] Couldn't connect to database. Retrying..."
            sleep(1)

def fetch_plates(connection):
    select_cursor = connection.cursor()
    query = 'SELECT id, plate, submitted FROM plate_readings'
    select_cursor.execute(query)
    results = select_cursor.fetchall()

    select_cursor.close()

    return results

def generate_post_body(result_row):
    row_id, plate, submitted = result_row

    body = {
        'plate_number': plate,
        'speed': 0,
        'event_time': submitted
    }

    return body

def post_plate(plate, on_success, *args):
    response = post_with_retry(data=body)

    if response.status_code == 201:
        print_post_info('succeeded', response.status_code, plate)

        on_success(*args)
    else:
        print_post_info('failed', response.status_code, plate)

def post_with_retry(data):
    while True:
        try:
            response = requests.post(API_COMPILER_ENDPOINT, data=data, headers={'Authorization': CLIENT_SECRET})
            return response
        except requests.exceptions.ConnectionError:
            print "[Error] Post failed. Retrying..."

def print_post_info(status, code, plate):
    print "[Info] Post {0}!".format(status)
    print "[Info] Code: {0}".format(code)
    print "[Info] Plate: {0}".format(plate)

def delete_plate_from_db(connection, plate_id):
    delete_cursor = connection.cursor()
    delete_query = 'DELETE FROM plate_readings WHERE id=' + str(plate_id)
    delete_cursor.execute(delete_query)
    connection.commit()
    delete_cursor.close()

def main():
    while True:
        connection = connect_to_db(CONN_PARAMS)

        results = fetch_plates(connection)

        for result_row in results:
            row_id = result_row[0]

            body = generate_post_body(result_row)
            post_plate(body['plate_number'], delete_plate_from_db, connection, row_id)

        connection.close()

if __name__ == '__main__':
    main()
