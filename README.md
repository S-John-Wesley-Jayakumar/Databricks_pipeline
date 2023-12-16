#Attached a Python file and Pyspark file which performs the ETL task by following below operations 

This code comprises several functions and a main function to perform the following tasks:

#Functions:
1)read_json_to_dataframe(json_file_path):

Reads a JSON file and converts it into a Pandas DataFrame.
Uses pd.read_json to read the JSON file and pd.json_normalize to flatten the nested JSON data.

2)item_least_rating(data):

Finds the item with the least rating from a provided DataFrame by using idxmin() to get the index of the item with the minimum rating.

3)item_most_rating(data):

Finds the item with the highest rating from a provided DataFrame by using idxmax() to get the index of the item with the maximum rating.

4)item_longest_review(data):

Identifies the item with the longest review text from a given DataFrame by applying the len function to the 'review_text' column and using idxmax() to get the index of the longest review.

5)transform_date(data):

Converts the 'old_date_col' column in the DataFrame to a new formatted 'new_date' column using pd.to_datetime and dt.strftime.

6)desired_dataframe_operation(data):

Computes the average rating per item by grouping the DataFrame based on 'item_id' and calculating the mean of the 'rating' column.

7)load_dataframe_to_mysql(dataframe, db_name, table_name, host, user, password):

Connects to a MySQL server and creates a database if it doesn't exist.
Creates a table in the specified database if it doesn't exist.
Loads the provided DataFrame into the MySQL table using SQLAlchemy's to_sql method.



8)Main Function (main()):

Reads a JSON file using read_json_to_dataframe.
Executes functions to find the item with the least/most rating, item with the longest review, and perform a desired operation on the DataFrame.
Transforms the DataFrame and saves it as a Parquet file.
Loads the Parquet data into a MySQL server using load_dataframe_to_mysql.
The main function coordinates the data processing, computation, transformation, file handling, and data loading into the MySQL database for further analysis or storage.
