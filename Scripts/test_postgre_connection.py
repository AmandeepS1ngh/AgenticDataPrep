import psycopg2
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME ="demo"
DB_USER = "postgres"
DB_PASS = "2004"

try:
    connection = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
    )
    cursor = connection.cursor()
    print("Connection to PostgreSQL database established successfully.")

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
    tables = cursor.fetchall()
    print("Tables in the database:")
    for table in tables:
        print(table[0])

    cursor.close()
    connection.close()
    print("PostgreSQL connection closed.")

except Exception as e:
    print(f"An error occurred: {e}")