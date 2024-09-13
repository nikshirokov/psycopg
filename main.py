import psycopg2
from psycopg2.sql import SQL, Identifier
from conf import db, user, password

def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE client_phone;
        DROP TABLE client_info;
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS client_info(
            client_id SERIAL PRIMARY KEY,
            client_first_name VARCHAR(40) NOT NULL,
            client_last_name VARCHAR(60) NOT NULL,
            client_email VARCHAR(100) UNIQUE
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS client_phone(
            id SERIAL PRIMARY KEY,
            phone_number VARCHAR(12) UNIQUE,
            client_id INTEGER NOT NULL REFERENCES client_info(client_id)
        );
        """)
        conn.commit()


def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO client_info(client_first_name,client_last_name,client_email)
        VALUES(%s, %s, %s)
        RETURNING client_id, client_first_name, client_last_name, client_email;
        """, (first_name, last_name, email))
        return cur.fetchone()


def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO client_phone(client_id,phone_number)
        VALUES(%s, %s)
        RETURNING client_id, phone_number;
        """, (client_id, phone))
        return cur.fetchone()


def change_client(conn, client_id, client_first_name=None, client_last_name=None, client_email=None):
    with conn.cursor() as cur:
        arg_list = {'client_first_name': client_first_name, "client_last_name": client_last_name,
                    'client_email': client_email}
        for key, arg in arg_list.items():
            if arg:
                cur.execute(SQL("UPDATE client_info SET {}=%s WHERE client_id=%s").format(Identifier(key)),
                            (arg, client_id))
        cur.execute("""
        SELECT * FROM client_info
        WHERE client_id=%s
        """, client_id)
        return cur.fetchall()


def delete_phone(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM client_phone
        WHERE client_id=%s
        RETURNING client_id
        """, (client_id,))
        return cur.fetchone()


def delete_client(conn, client_id):
    delete_phone(conn, client_id)
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM client_info
        WHERE client_id = %s
        """, (client_id,))


def find_client(conn, client_first_name=None, client_last_name=None, client_email=None, phone_number=None):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT * FROM client_info c
        LEFT JOIN client_phone cp ON c.client_id = cp.client_id
        WHERE (client_first_name = %(client_first_name)s OR %(client_first_name)s IS NULL)
        AND (client_last_name = %(client_last_name)s OR %(client_last_name)s IS NULL)
        AND (client_email = %(client_email)s OR %(client_email)s IS NULL)
        AND (phone_number = %(phone_number)s OR %(phone_number)s IS NULL);
        """, {'client_first_name': client_first_name, 'client_last_name': client_last_name,
              'client_email': client_email, 'phone_number': phone_number})
        return cur.fetchone()


with psycopg2.connect(database=db, user=user, password=password) as conn:
    create_db(conn)
    print(add_client(conn, 'Nikolay', 'Shirokov', 'nsh@internet.ru'))
    print(add_client(conn, 'Alexandr', 'Zubarev', 'zu@internet.ru'))
    print(add_client(conn, 'Ksenya', 'Shirokova', 'ks@internet.ru'))
    print(add_phone(conn, 1, 89693239999))
    print(add_phone(conn, 2, 89993790000))
    print(add_phone(conn, 2, 83332323055))
    print(change_client(conn, '1', 'Taty', 'Shirokova', 'shirokova@mail.ru'))
    print(delete_phone(conn, '2'))
    delete_client(conn, '1')
    print(find_client(conn, 'Ksenya'))
conn.close()
