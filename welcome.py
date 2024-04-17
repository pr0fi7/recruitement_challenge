import streamlit as st
import sqlite3
import pandas as pd

# Connect to SQLite database
conn1 = sqlite3.connect('data.db')  # Replace 'your_database.db' with the path to your SQLite database file

# Create a function to execute SQL queries
def run_query_without(query):
    cursor = conn1.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    return results

# Streamlit app
def main():
    st.title("Streamlit SQL Example")

    # Sample SQL query
    query = '''
        SELECT COUNT(*) FROM data
    '''

    # Execute the query
    results = run_query_without(query)

    # Display the results
    st.write("Result of SQL query:")
    st.write(results)

if __name__ == "__main__":
    main()
