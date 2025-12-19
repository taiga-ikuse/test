import sqlite3
import csv
import os

def run_query(db_path: str, sql: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    return rows

def write_csv(output_path: str, rows):
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

def main():
    base_dir = os.getcwd()

    db_path = os.path.join(base_dir, "data", "database_02.sqlite")
    output_path = os.path.join(base_dir, "data", "accesslog_02.answer.csv")

    sql = """
    WITH ip_by_hour AS (
  SELECT
    REQUEST_TIME,
    IP_ADDRESS,
    COUNT(*) AS NN
  FROM 
    access_log
  GROUP BY 
    REQUEST_TIME, 
    IP_ADDRESS
),

filtered AS (
  SELECT
    REQUEST_TIME,
    NN
  FROM ip_by_hour
  WHERE NN < 10
)
SELECT
  REQUEST_TIME,
  COALESCE(SUM(NN), 0) AS NN
FROM 
  filtered
GROUP BY 
  REQUEST_TIME
ORDER BY
  REQUEST_TIME;
    """

    try:
        rows = run_query(db_path, sql)
        write_csv(output_path, rows)
        print(f"Output written to {output_path}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
