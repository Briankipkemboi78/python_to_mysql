import pandas as pd
import oracledb as cx_Oracle
import mysql.connector
import csv
from datetime import datetime
from mysql.connector import Error
 
# Reading connection credentials from file
creds = pd.read_csv(r"####", header=None, sep='|') # This is the path to the creds file
 
# List of Oracle queries and corresponding MySQL table names
oracle_queries = [
    ( """
    WITH accounts
    AS (
        SELECT
                orgkey,
                CASE
                    WHEN cust_swift_code_desc IS NULL AND struserfield8 IS NOT  NULL THEN struserfield8
                    WHEN cust_swift_code_desc IS NOT NULL AND struserfield8 IS NULL THEN cust_swift_code_desc
                    ELSE NULL
                END  "KRA_PIN",
                relationshipopeningdate AS CIF_OPEN_DATE
            FROM ugedw.stg_accounts  
            WHERE bank_id = '54'
        ),
    gam AS (
        SELECT
                gam.cif_id,
                gam.sol_id,
                sol.sol_desc AS branch_name,
                gam.foracid,
                gam.acct_name,
                gam.schm_code,
                schm.schm_desc,
                gam.acct_opn_date
            FROM ugedw.stg_gam    gam
            LEFT JOIN ugedw.stg_sol    sol ON gam.sol_id = sol.sol_id
            LEFT JOIN ugedw.schm_desc    schm ON gam.schm_code = schm.schm_code
            WHERE
                (SYSDATE - gam.acct_opn_date )<= 7  AND  
                gam.schm_type IN ('CAA', 'SBA')AND
                gam.schm_code != 'VA300' AND
                gam.bank_id = '54'
        )
SELECT
    k.*
    FROM
        (
            SELECT
                g.cif_id,
                g.sol_id,
                g.branch_name,
                g.foracid,
                g.acct_name,
                g.schm_code,
                g.schm_desc,
                acc.kra_pin,
                DECODE(LENGTH(TRIM(acc.kra_pin)),11, 'Valid','Invalid')AS kra_pin_status,
                g.acct_opn_date,
                acc.cif_open_date
            FROM gam g
            LEFT JOIN accounts acc ON g.cif_id = acc.orgkey )k
    WHERE
        kra_pin_status = 'Invalid' AND
        sol_id != '777' AND
        schm_code NOT IN('SB103','SB108','SB110','SB113','SB114','SB122','SB130','SB136','SB137','SB139','SB140','SB142','SB145','SB146')
    """, 'Invalid_KRA_PINs'),
    ("""WITH accounts
    AS (
        SELECT
            orgkey,
            nat_id_card_num,
            CASE
                WHEN REGEXP_LIKE(TRIM(nat_id_card_num),'^[0-9]{7}$|^[0-9]{8}$')
                    OR passportno IS NOT NULL
                THEN ' Valid'
                ELSE 'Invalid'
            END  AS ID_Status,
            CASE
                WHEN salutation = 'M/S' THEN 'Y'
                WHEN corp_id IS NOT NULL THEN 'Y'
                WHEN nvl(gender,'O') NOT IN ('M','F') then 'Y'
                ELSE 'N'
            END AS Institutional,
            relationshipopeningdate AS CIF_OPEN_DATE
            FROM ugedw.stg_accounts  
            WHERE bank_id = '54'
        ),
    gam AS (
        SELECT
                gam.cif_id,
                gam.sol_id,
                sol.sol_desc AS branch_name,
                gam.foracid,
                gam.acct_name,
                gam.schm_code,
                schm.schm_desc,
                gam.acct_opn_date
            FROM ugedw.stg_gam  gam
            LEFT JOIN ugedw.stg_sol    sol ON gam.sol_id = sol.sol_id
            LEFT JOIN ugedw.schm_desc    schm ON gam.schm_code = schm.schm_code
            WHERE
                (gam.acct_opn_date BETWEEN (TRUNC(SYSDATE, 'Month') - INTERVAL '0' MONTH) AND SYSDATE) AND
                gam.schm_type IN ('CAA', 'SBA')AND
                gam.schm_code != 'VA300' AND
                gam.bank_id = '54'
        )
SELECT
    k.*
    FROM
        (
            SELECT
                g.cif_id,
                g.sol_id,
                g.branch_name,
                g.foracid,
                g.acct_name,
                g.schm_code,
                g.schm_desc,
                acc.nat_id_card_num,
                acc.Institutional,
                acc.ID_Status,
                g.acct_opn_date,
                acc.cif_open_date
            FROM gam g
            LEFT JOIN accounts acc ON g.cif_id = acc.orgkey )k
    WHERE
        ID_Status = 'Invalid'
        AND Institutional = 'N'
        AND schm_code not in ('CA203','SB122','SB124','SB103','SB151','SB152','SB153','SB142','SB190','SB130','SB145','SB145','SB146','SB115','SB137','SB110','SB102','SB113','SB114','SB199','130','SB136','199','SB444''SB102','SB121','SB140','CA210','SB777')
    """, 'Invalid_IDs'),
    ("""SELECT
    a.ACID,
    a.SOL_ID,
    sol.sol_desc AS BRANCH_NAME,
    d.REGION,
    a.FORACID,
    a.ACCT_NAME,
    c.NOM_NAME,
    a.SCHM_CODE,
    TRUNC(a.ACCT_OPN_DATE) AS ACCT_OPN_DATE,
    a.CLR_BAL_AMT ,
    a.MODE_OF_OPER_CODE
FROM ugedw.stg_gam a
LEFT JOIN ugedw.stg_smt b ON a.acid=b.acid
LEFT JOIN ugedw.stg_ant c ON c.acid = a.acid
LEFT JOIN ugedw.BRANCH_DETAILS d ON a.SOL_ID = d.SOL_ID
LEFT JOIN ugedw.stg_sol sol ON a.SOL_ID = sol.SOL_ID
WHERE a.bank_id = '54'
    AND a.schm_code in ('SB100','SB109','SB777')
    AND a.MODE_OF_OPER_CODE='SELF'
    AND a.acct_cls_flg = 'N'
    AND a.SOL_ID <> '777'
    AND c.NOM_NAME IS NULL
    AND a.acct_opn_date >= TRUNC(SYSDATE, 'Month') - INTERVAL '2' MONTH
    AND a.acct_opn_date <= SYSDATE
    ORDER BY ACCT_OPN_DATE ASC""", 'Without_Nominee_Details')
]
 
# Establishing connection to Oracle
try:
    dsn = cx_Oracle.makedsn('db-clstr3-scan', 1590, service_name=creds.iat[0, 2])
    oracle_conn = cx_Oracle.connect(user=creds.iat[1, 0], password=creds.iat[1, 1], dsn=dsn)
    print("[+] Oracle Connection successfully initiated")
 
    # Oracle cursor creation
    oracle_cursor = oracle_conn.cursor()
    oracle_cursor.arraysize = 10000  # Adjust for performance
 
    # Establishing connection to MySQL
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password=creds.iat[2, 1],  # Use the password from the file
        database=creds.iat[2, 0]   # Use the database name from the file
    )
    print("[+] MySQL Connection successfully initiated")
   
    # Create a cursor for MySQL
    mysql_cursor = conn.cursor()
 
    # Loop through each Oracle query and corresponding MySQL table
    for query, table_name in oracle_queries:
        try:
            # Execute Oracle query
            print(f"Fetching data from Oracle for table '{table_name}'...")
            oracle_cursor.execute(query)
 
            # Fetch all results
            data = oracle_cursor.fetchall()
 
            # Get the column names
            columns = [col[0] for col in oracle_cursor.description]
 
            # Create a DataFrame from the Oracle query result
            df = pd.DataFrame(data, columns=columns)
 
            # Convert timestamp columns to strings in 'YYYY-MM-DD HH:MM:SS' format
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
 
            print(f"[+] Fetched {len(df)} rows from Oracle for table '{table_name}'")
 
            # Create the MySQL table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join([f'{col} VARCHAR(255)' for col in columns])}
            );
            """
            mysql_cursor.execute(create_table_query)
 
            # Insert data into MySQL table
            insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({', '.join(['%s' for _ in columns])})
            """
           
            # Insert each row of the DataFrame into the MySQL table
            for row in df.itertuples(index=False):
                mysql_cursor.execute(insert_query, tuple(row))
 
            # Commit the transaction after inserting data for each table
            conn.commit()
 
            print(f"[+] Data successfully inserted into MySQL table '{table_name}' ({df.shape[0]} rows inserted)")
 
        except Exception as err:
            print(f"[!] Error processing table '{table_name}': {err}")
   
except cx_Oracle.DatabaseError as err:
    print(f"[!] Oracle Connection Error: {err}")
    exit()
 
except Error as err:
    print(f"[!] MySQL Connection Error: {err}")
    exit()
 
finally:
    # Close Oracle cursor and connection
    if 'oracle_cursor' in locals():
        oracle_cursor.close()
    if 'oracle_conn' in locals():
        oracle_conn.close()
 
    # Close MySQL cursor and connection
    if 'mysql_cursor' in locals():
        mysql_cursor.close()
    if 'conn' in locals():
        conn.close()