import psycopg2

conn_params = {
    'dbname': 'feature_store',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'localhost',
    'port': 5432
}

data = [
    (6, 148, 72, 35, 0, 33.6, 0.627, 50, 1),
    (1, 85, 66, 29, 0, 26.6, 0.351, 31, 0),
    # Add more rows as needed
]

try: 
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO features ( pregnancies, glucose, bloodpressure, skinthickness, insulin, bmi, diabetespedigreefunction, age, outcome) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, data)

    # Commit the transaction
    conn.commit()

    print("Data inserted successfully")
except Exception as error:
    print(error)
finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()