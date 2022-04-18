import os
import psycopg2


def query(conn, q):
    cur = conn.cursor()
    cur.execute(q)
    res = cur.fetchall()
    cur.close()
    return res


def get_table_files(user, password, db):
    conn = psycopg2.connect(
        host="localhost",
        database=db,
        user=user,
        password=password)

    q = """select setting from pg_settings where name = 'data_directory'"""
    res = query(conn, q)
    root_dir = res[0][0]

    q = f"""SELECT oid FROM pg_database WHERE datname = '{db}'"""
    res = query(conn, q)
    oid = str(res[0][0])

    q = f"""
    SELECT T.table_name, P.oid FROM information_schema.tables T JOIN pg_class P 
        ON T.table_name = P.relname
        WHERE table_schema = 'public' AND P.relfilenode != 0;
    """
    res = query(conn, q)
    table_files = {k: os.path.join(root_dir, 'base', oid, str(v)) for k, v in res}

    conn.close()

    return table_files


table_files = get_table_files(user='postgres', password='12345', db='postgres')

print(table_files)
print(table_files.items())
