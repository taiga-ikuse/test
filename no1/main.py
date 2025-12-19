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

    db_path = os.path.join(base_dir, "data", "database_01.sqlite")
    output_path = os.path.join(base_dir, "data", "accesslog_01.answer.csv")

    sql = """
 WITH bot AS (
      SELECT
        DISTINCT BOT_IP_ADDRESS AS IP_ADDRESS
      FROM
        bot_ip_address

    ),
    base_table AS (
      SELECT
        SUBSTR(a.REQUEST_TIME, 1, 8) AS REQUEST_DATE,
        COUNT(1) AS TOTAL_NN,
        SUM(CASE WHEN b.IP_ADDRESS IS NULL THEN 1 ELSE 0 END) AS NN
      FROM
        access_log AS a
      LEFT JOIN
        bot AS  b
      USING
        (IP_ADDRESS)
      GROUP BY
        SUBSTR(a.REQUEST_TIME, 1, 8)
    )
    SELECT
      REQUEST_DATE,
      NN
    FROM 
      base_table
    WHERE 
      -- 正常アクセス数が5割(50%)以上
      NN * 2 >= TOTAL_NN
    ORDER BY 
      REQUEST_DATE ASC;
"""

    try:
        rows = run_query(db_path, sql)
        write_csv(output_path, rows)
        print(f"Output written to {output_path}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()