import mysql.connector
import requests
from time import sleep

API_HOST = 'http://proyectozapo.herokuapp.com/api/v1'
API_COMPILER_ENDPOINT = '{0}/vehicle_event'.format(API_HOST)
CLIENT_SECRET = 'eyJhbGciOiJIUzI1NiJ9.eyJidXNfc3RvcF9jb2RlIjoiUEMxNjQifQ.lzAAAxhzXbFmjLLbgV5mIvTSt9l7417pzpmzP0YkqCM'

CONN_PARAMS = {
  'user': 'ALPR',
  'password': 'PASSALPR',
  'host': 'docker-db',
  'database': 'control_point'
}

while True:
    try:
        print "Connecting to db"
        connection = mysql.connector.connect(buffered=True, **CONN_PARAMS)
        select_cursor = connection.cursor()

        query = 'SELECT id, plate, submitted FROM plate_readings'
        select_cursor.execute(query)
        results = select_cursor.fetchall()

        for (row_id, plate, submitted) in results:

            body = {
                'plate_number': plate,
                'speed': 0,
                'event_time': submitted
            }

            response = requests.post(API_COMPILER_ENDPOINT, data=body, headers={'Authorization': 'Bearer {0}'.format(CLIENT_SECRET)})

            if response.status_code == 201:
                print "Plate {0}: [201]".format(plate)

                delete_cursor = connection.cursor()
                delete_query = 'DELETE FROM plate_readings WHERE id=' + str(row_id)
                delete_cursor.execute(delete_query)
                connection.commit()
                delete_cursor.close()

            else:
                print "Should retry"

        select_cursor.close()
        connection.close()
    except mysql.connector.errors.InterfaceError:
        print "Couldn't connect to database. Retrying..."
    sleep(1)