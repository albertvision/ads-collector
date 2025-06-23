import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MIGRATIONS_DIR = os.getenv("MIGRATIONS_DIR", "migrations")


conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE,
)
cursor = conn.cursor()

# table to track applied migrations
cursor.execute(
    "CREATE TABLE IF NOT EXISTS schema_migrations (filename VARCHAR(255) PRIMARY KEY)"
)

# get applied migrations
cursor.execute("SELECT filename FROM schema_migrations")
applied = {row[0] for row in cursor.fetchall()}

files = sorted(f for f in os.listdir(MIGRATIONS_DIR) if f.endswith('.sql'))

for fname in files:
    if fname in applied:
        continue
    path = os.path.join(MIGRATIONS_DIR, fname)
    with open(path, 'r') as f:
        sql = f.read()
    for statement in [s.strip() for s in sql.split(';') if s.strip()]:
        cursor.execute(statement)
    cursor.execute(
        "INSERT INTO schema_migrations (filename) VALUES (%s)",
        (fname,)
    )
    print(f"Applied migration {fname}")

conn.commit()
cursor.close()
conn.close()
