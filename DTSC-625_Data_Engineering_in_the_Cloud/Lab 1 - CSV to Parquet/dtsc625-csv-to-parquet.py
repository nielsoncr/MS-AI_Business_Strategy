import boto3
import awswrangler as wr
from urllib.parse import unquote_plus

def lambda_handler(event, context):
    # Get the source bucket and object name as passed to the Lambda function
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = unquote_plus(record['s3']['object']['key'])

    # We will set the DB and table name based on the last two elements of
    # the path prior to the file name. If key = '625-labs/eicher-table/data.csv',
    # then the following lines will set db to ‘625-labs’ and table_name to 'eicher-table'

    key_list = key.split("/")
    print(f'key_list: {key_list}')
    
    # Handle edge case where the S3 path might be too shallow
    if len(key_list) < 3:
        print(f"ERROR: Key path '{key}' is too short to extract DB and Table names.")
        return None 
    
    db_name = key_list[len(key_list)-3]
    table_name = key_list[len(key_list)-2]

    # Print for error handling
    print(f'Bucket: {bucket}')
    print(f'Key: {key}')
    print(f'DB Name: {db_name}')
    print(f'Table Name: {table_name}')

    input_path = f"s3://{bucket}/{key}"
    output_path = f"s3://dtsc625-eicher-clean-zone/{db_name}/{table_name}" # pdate bucket name!
    print(f'Input_Path: {input_path}')
    print(f'Output_Path: {output_path}')

    # Read the CSV and do prints for error handling
    input_df = wr.s3.read_csv([input_path])
    print(input_df.head(3))
    print(f'Input DataFrame Shape: {input_df.shape}')
    print(f'Input DataFrame Columns: {input_df.columns}')

    # Ensure Glue database exists
    current_databases = wr.catalog.databases()
    if db_name not in current_databases.values:
        print(f'- Database {db_name} does not exist ... creating')
        wr.catalog.create_database(db_name)
    else:
        print(f'- Database {db_name} already exists')

    # Convert the CSV to Parquet and store it in database with the given table name
    # We overwrite (rather than append) mainly for error handling;
    # in production, append might be more appropriate
    result = wr.s3.to_parquet(

     df=input_df,
        path=output_path,
        dataset=True,
        database=db_name,
        table=table_name,
        mode="overwrite" #instead of append
        ) 

    print("RESULT: ")
    print(f'{result}')

    return result