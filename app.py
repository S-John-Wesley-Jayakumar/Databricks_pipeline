import pandas as pd

def read_json_to_dataframe(json_file_path):
    try:
        # Read JSON file into a DataFrame       
        # If the JSON file contains nested data and you want to flatten it:
        df = pd.json_normalize(pd.read_json(json_file_path, lines=True))

        return df

    except Exception as e:
        print(f"Error occurred: {e}")
        return None



def item_least_rating(data):
    least_rated_item = data.loc[data['rating'].idxmin()]
    return least_rated_item.to_dict() if not data.empty else None

def item_most_rating(data):
    most_rated_item = data.loc[data['rating'].idxmax()]
    return most_rated_item.to_dict() if not data.empty else None

def item_longest_review(data):
    longest_review_item = data.loc[data['review_text'].apply(len).idxmax()]
    return longest_review_item.to_dict() if not data.empty else None

def transform_date(data):
    data['new_date'] = pd.to_datetime(data['old_date_col']).dt.strftime("%m-%d-%Y")
    return data

def desired_dataframe_operation(data):
    avg_rating_per_item = data.groupby('item_id')['rating'].mean()
    return avg_rating_per_item.to_dict()


def load_dataframe_to_mysql(dataframe, db_name, table_name, host, user, password):
    import mysql.connector
    from mysql.connector import Error
    import pandas as pd
    from sqlalchemy import create_engine

    try:
        # Connect to the MySQL server
        conn = mysql.connector.connect(host=host, user=user, password=password)
        cursor = conn.cursor()

        # Create the database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        cursor.execute(f"USE {db_name}")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Create the table if it doesn't exist
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (item_id INT, rating INT, review_text TEXT, old_date_col DATE)" # Define your table schema
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' created or already exists.")

        # Exclude 'new_date' column from DataFrame
        dataframe_without_new_date = dataframe.drop(columns=['new_date'])

        # Load modified DataFrame into MySQL table
        engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{db_name}")
        dataframe_without_new_date.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print(f"Data loaded from DataFrame into '{table_name}' in the '{db_name}' database.")

    except Error as e:
        print(f"Error: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def main():
    
    file_locaton = f"H:\DEEEEEEEEEEEEEEEEEEEEE\australian_user_reviews copy.json"
    sample_data = read_json_to_dataframe(file_locaton)

    least_rated_item = item_least_rating(sample_data)
    if least_rated_item:
        print("Item with the least rating:")
        print(least_rated_item)
    else:
        print("No data found for item with the least rating.")

    most_rated_item = item_most_rating(sample_data)
    if most_rated_item:
        print("\nItem with the most rating:")
        print(most_rated_item)
    else:
        print("No data found for item with the most rating.")

    longest_review_item = item_longest_review(sample_data)
    if longest_review_item:
        print("\nItem with the longest review:")
        print(longest_review_item)
    else:
        print("No data found for item with the longest review.")


    transformed_data = transform_date(sample_data)

    desired_operation_result = desired_dataframe_operation(transformed_data)

    if desired_operation_result:
        print("\nDesired DataFrame operation result:Average")
        print(desired_operation_result)
    else:
        print("No data found for desired DataFrame operation.")


   
    file_path = "output_data.parquet"
    transformed_data.to_parquet(file_path)  # Save DataFrame to Parquet file
    print("DataFrame saved successfully as a Parquet file.")


     # Read and display the Parquet file
    loaded_df = pd.read_parquet(file_path)
    print("\nDisplaying the Loaded DataFrame from Parquet file:")
    print(loaded_df)
    

    # Load Parquet data into SQL Server
   

    df =loaded_df
    db_name= 'mydb'
    host='localhost'
    user='root'
    password='password'
    table_name = 'table_name'  # Provide the name of your SQL Server table


    load_dataframe_to_mysql(df,db_name, table_name, host, user, password)


if __name__ == "__main__":
    main()
