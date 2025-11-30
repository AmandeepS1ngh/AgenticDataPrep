import pymysql
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("MYSQL_PORT", 3306))
DB_NAME = os.getenv("MYSQL_DATABASE", "demo")
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASS = os.getenv("MYSQL_PASSWORD", "")

print(f"🔌 Connecting to MySQL at {DB_HOST}:{DB_PORT}...")
print(f"📁 Database: {DB_NAME}")
print(f"👤 User: {DB_USER}")

try:
    connection = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )
    cursor = connection.cursor()
    print("\n✅ Connection to MySQL successful!")

    # Show tables
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()
    print("\n📋 Tables in database:")
    for table in tables:
        print(f"   └── {table[0]}")

    # Show sample data
    cursor.execute("SELECT * FROM customers LIMIT 5;")
    rows = cursor.fetchall()
    print("\n📊 Sample data from 'customers':")
    for row in rows:
        print(f"   {row}")

    cursor.close()
    connection.close()
    print("\n✅ Connection closed.")

except Exception as e:
    print(f"\n❌ Error: {e}")