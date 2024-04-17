import sqlite3 as sql
import pandas as pd
from sklearn.utils import resample
import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt


data = pd.read_parquet('/Users/markshevchenkopu/BXL-Bouman-7/projects/08-DS-recruitment-challenge/transactions.parquet', engine='fastparquet')

con = sql.connect('data1.db')
data.to_sql('data1', con, if_exists= 'replace', index=False)


df_balanced = pd.read_sql_query('''
WITH q_user AS (
    SELECT CustomerID, COUNT(SaleDocumentNumber) AS q_per_user
    FROM data1
    GROUP BY CustomerID
),
q_invoice AS (
    SELECT CustomerID, SaleDocumentNumber, COUNT(SaleDocumentNumber) AS q_per_invoice
    FROM data1
    GROUP BY SaleDocumentNumber
), combined AS (
                  SELECT q_user.CustomerID, q_user.q_per_user, q_invoice.SaleDocumentNumber, q_invoice.q_per_invoice
                  FROM q_user
                  JOIN q_invoice
                  ON q_invoice.CustomerID = q_user.CustomerID)
SELECT q_per_user, q_per_invoice, data1.*
FROM combined
JOIN data1
ON combined.CustomerID = data1.CustomerID and combined.SaleDocumentNumber = data1.SaleDocumentNumber
                  ''', con)

con.close()

con = sql.connect('df_final.db')
df_balanced.to_sql('df_final', con, if_exists= 'replace', index=False)


# Define SQL queries
query1 = '''
SELECT q_per_invoice, COUNT(Returned) AS count_return
FROM df_final
WHERE Returned = 1
GROUP BY q_per_invoice
ORDER BY count_return DESC
'''

query2 = '''
SELECT q_per_user, COUNT(Returned) AS count_return
FROM df_final
WHERE Returned = 1
GROUP BY q_per_user
ORDER BY count_return DESC
'''

# Execute queries and display results
for index, query in enumerate([query1, query2], start=1):
    df = pd.read_sql_query(query, con)
    st.write(f"Result of SQL query {index}:")
    st.write(df)

# Close the connection
con.close()


con = sql.connect('data.db')
data.to_sql('data', con, if_exists= 'replace', index=False)
connection = sql.connect('data.db')

query1 = (''' Select Shop, count(*) as count_returned
                  from data
                  where Returned = 1
                  group by Shop
                  order by count_returned DESC''')

query2 = ('''SELECT CustomerID, COUNT(SaleDocumentNumber) AS count_returned
                  FROM data
                  GROUP BY CustomerID
                  Order by count_returned DESC''')

query3 = (''' SELECT ProductCode, COUNT(SaleDocumentNumber) AS count_returned
                  FROM data
                  GROUP BY ProductCode
                  Order by count_returned DESC''')

query4 = (''' SELECT BrandName, COUNT(SaleDocumentNumber) AS count_returned
                  FROM data
                  GROUP BY BrandName
                  Order by count_returned DESC''')

query5 = (''' SELECT ProductCode, CostPriceExclVAT, COUNT(SaleDocumentNumber) AS count_returned
                  FROM data
                  GROUP BY ProductCode
                  ORDER BY count_returned DESC''')

query6= (''' SELECT CostPriceExclVAT, COUNT(SaleDocumentNumber) AS count_returned
                  FROM data
                  GROUP BY CostPriceExclVAT
                  ORDER BY CostPriceExclVAT DESC''')

query7 = ('''
WITH sales AS (
    SELECT Shop, SUM(OriginalSaleAmountInclVAT) as sum 
    FROM data
    WHERE Returned = 1
    GROUP BY Shop
),
purchases AS (
    SELECT Shop, SUM(OriginalSaleAmountInclVAT) as sum 
    FROM data
    GROUP BY Shop
)

SELECT sales.Shop, sales.sum as total_return_sales, purchases.sum as total_purchases, sales.sum / purchases.sum as return_rate
FROM sales
JOIN purchases ON sales.Shop = purchases.Shop
ORDER BY return_rate DESC
''')
            
# def display_results(df, query_index):
#     if len(df.columns) == 2:
#         plt.figure(figsize=(10, 6))
#         sns.scatterplot(data=df, hue = df.columns[0], y=df.columns[1])
#         plt.title(f"Result of SQL query {query_index}")
#         plt.xlabel(df.columns[0])
#         plt.ylabel(df.columns[1])
#         st.pyplot(fig=plt)

# Execute queries and display results
for index, query in enumerate([query1, query2, query3, query4, query5, query6, query7], start=1):
    df = pd.read_sql_query(query, connection)
    
# import sqlite3 as sql
# import pandas as pd
# from sklearn.utils import resample
# import streamlit as st

# def prepreprocess_data(data_path):
#     data = pd.read_parquet(data_path, engine='fastparquet')
#     con = sql.connect('data.db')
#     data.to_sql('data', con, if_exists= 'replace', index=False)
#     connection = sql.connect('data.db')

#     df_balanced = pd.read_sql_query('''
#     WITH q_user AS (
#         SELECT CustomerID, COUNT(SaleDocumentNumber) AS q_per_user
#         FROM data
#         GROUP BY CustomerID
#     ),
#     q_invoice AS (
#         SELECT CustomerID, SaleDocumentNumber, COUNT(SaleDocumentNumber) AS q_per_invoice
#         FROM data
#         GROUP BY SaleDocumentNumber
#     ), combined AS (
#                     SELECT q_user.CustomerID, q_user.q_per_user, q_invoice.SaleDocumentNumber, q_invoice.q_per_invoice
#                     FROM q_user
#                     JOIN q_invoice
#                     ON q_invoice.CustomerID = q_user.CustomerID)
#     SELECT q_per_user, q_per_invoice, data.*
#     FROM combined
#     JOIN data
#     ON combined.CustomerID = data.CustomerID and combined.SaleDocumentNumber = data.SaleDocumentNumber
#                     ''', connection)
#     return df_balanced

# def preprocess_data(data_path):
#     data = prepreprocess_data(data_path)

#     df_1 = data[data['Returned'] == 1]
#     other_df = data[data['Returned'] == 0]

#     df_other_oversampled_training = resample(other_df, n_samples=900000, random_state=42)

#     df_balanced = pd.concat([df_1, df_other_oversampled_training])
#     df_balanced = df_balanced.sample(frac=1, random_state=42).reset_index(drop=True)

#     columns_to_keep = ['RevenueInclVAT', 'Returned', 'CostPriceExclVAT', 'OriginalSaleAmountInclVAT']
#     columns_to_convert = df_balanced.columns.difference(columns_to_keep)
#     df_balanced[columns_to_convert] = df_balanced[columns_to_convert].astype('category')

#     return df_balanced

# def preprocess_data_2(data_path):
#     data = pd.read_parquet(data_path, engine='fastparquet')
#     columns_to_keep = ['RevenueInclVAT', 'Returned', 'CostPriceExclVAT', 'OriginalSaleAmountInclVAT']
#     columns_to_convert = data.columns.difference(columns_to_keep)
#     data[columns_to_convert] = data[columns_to_convert].astype('category')
#     return data

# def save_to_database(df, db_name, table_name):
#     con = sql.connect(db_name)
#     df.to_sql(table_name, con, if_exists='replace', index=False)
#     con.close()

# def execute_query_and_display_results(connection, query, index):
#     df = pd.read_sql_query(query, connection)
#     st.write(f"Result of SQL query {index}:")
#     st.write(df)

# def main():
#     data_path = '/Users/markshevchenkopu/BXL-Bouman-7/projects/08-DS-recruitment-challenge/transactions.parquet'
#     df_balanced = preprocess_data(data_path)
#     df_balanced_db_name = 'df_balanced.db'
#     df_balanced_table_name = 'df_balanced'

#     df_data = preprocess_data_2(data_path)
#     df_data_db_name = 'df_data.db'
#     df_data_table_name = 'df_data'


#     # Save balanced data to database
#     save_to_database(df_balanced, df_balanced_db_name, df_balanced_table_name)

#     # Connect to the database
#     connection = sql.connect(df_balanced_db_name)

#     # Define SQL queries
#     queries = [
#         f'''
#         SELECT q_per_invoice, COUNT(Returned) AS count_return
#         FROM {df_balanced_table_name}
#         WHERE Returned = 1
#         GROUP BY q_per_invoice
#         ORDER BY count_return DESC
#         ''',
#         f'''
#         SELECT q_per_user, COUNT(Returned) AS count_return
#         FROM {df_balanced_table_name}
#         WHERE Returned = 1
#         GROUP BY q_per_user
#         ORDER BY count_return DESC
#         '''
#     ]

#     # Execute queries and display results
#     for index, query in enumerate(queries, start=1):
#         execute_query_and_display_results(connection, query, index)

#     # Close the connection
#     connection.close()
#     save_to_database(df_data, df_data_db_name, df_data_table_name)

#     # Connect to the data database
#     connection = sql.connect(f'{df_data_db_name}')

#     # Define SQL queries for data database
#     queries = [
#         f''' 
#         SELECT Shop, COUNT(*) as count_returned
#         FROM {df_data_table_name}
#         WHERE Returned = 1
#         GROUP BY Shop
#         ORDER BY count_returned DESC
#         ''',
#         f'''
#         SELECT CustomerID, COUNT(SaleDocumentNumber) AS count_returned
#         FROM {df_data_table_name}
#         GROUP BY CustomerID
#         ORDER BY count_returned DESC
#         ''',
#         f''' SELECT ProductCode, COUNT(SaleDocumentNumber) AS count_returned
#                   FROM {df_data_table_name}
#                   GROUP BY ProductCode
#                   Order by count_returned DESC''',
# f''' SELECT BrandName, COUNT(SaleDocumentNumber) AS count_returned
#                   FROM {df_data_table_name}
#                   GROUP BY BrandName
#                   Order by count_returned DESC''',

# f''' SELECT ProductCode, CostPriceExclVAT, COUNT(SaleDocumentNumber) AS count_returned
#                   FROM {df_data_table_name}
#                   GROUP BY ProductCode
#                   ORDER BY count_returned DESC''',

# f''' SELECT CostPriceExclVAT, COUNT(SaleDocumentNumber) AS count_returned
#                   FROM {df_data_table_name}
#                   GROUP BY CostPriceExclVAT
#                   ORDER BY CostPriceExclVAT DESC''',
# f'''
# WITH sales AS (
#     SELECT Shop, SUM(OriginalSaleAmountInclVAT) as sum 
#     FROM {df_data_table_name}
#     WHERE Returned = 1
#     GROUP BY Shop
# ),
# purchases AS (
#     SELECT Shop, SUM(OriginalSaleAmountInclVAT) as sum 
#     FROM {df_data_table_name}
#     GROUP BY Shop
# )

# SELECT sales.Shop, sales.sum as total_return_sales, purchases.sum as total_purchases, sales.sum / purchases.sum as return_rate
# FROM sales
# JOIN purchases ON sales.Shop = purchases.Shop
# ORDER BY return_rate DESC
# '''
#     ]

#     # Execute queries and display results
#     for index, query in enumerate(queries, start=1):
#         execute_query_and_display_results(connection, query, index)

#     # Close the connection
#     connection.close()

# if __name__ == "__main__":
#     main()


