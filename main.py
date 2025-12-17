import sqlite3
import csv
import os

def main():
    # パスの設定 (run.shの実行場所を基準にdataディレクトリを参照)
    dirname = os.getcwd()
    db_path = os.path.join(dirname, 'data', 'database_01.sqlite')
    output_path = os.path.join(dirname, 'data', 'accesslog_01.answer.csv')

    # 実行するSQL
    # SQLiteの関数(SUBSTR)を使用し、Python内で実行します
    sql_query = """
    WITH bot AS (
      SELECT
        DISTINCT BOT_IP_ADDRESS AS ip
      FROM
        bot_ip_address
    ),
    base_table AS (
      SELECT
        SUBSTR(a.REQUEST_TIME, 1, 8) AS request_date,
        COUNT(1) AS total_cnt,
        SUM(CASE WHEN b.ip IS NULL THEN 1 ELSE 0 END) AS normal_cnt
      FROM
        access_log a
      LEFT JOIN
        bot b
        ON a.IP_ADDRESS = b.ip
      GROUP BY
        SUBSTR(a.REQUEST_TIME, 1, 8)
    )
    SELECT
      request_date,
      normal_cnt
    FROM 
      base_table
    WHERE 
      -- 正常アクセス数が5割(50%)以上
      normal_cnt * 2 >= total_cnt
    ORDER BY 
      request_date ASC;
    """

    try:
        # データベース接続
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # SQL実行
        cursor.execute(sql_query)
        results = cursor.fetchall()

        # CSV出力
        # lineterminator='\n' はLinux/Unix環境(採点環境など)での標準的な改行コードに合わせるため推奨
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, lineterminator='\n')
            
            # ヘッダーは出力しない要件のため、データのみ書き込む
            writer.writerows(results)

        print(f"Successfully processed data and wrote to {output_path}")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()