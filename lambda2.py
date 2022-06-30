import json
import logging # manda logs al clouldwatch service
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

destination_bucket = 'devopslatam02-result-lb'
destination_folder = 'publications'

athena_staging_table = 'publications_staging'
athena_staging_database = 'devopslatam02'
athena_s3_results_folder = 'athena_results'

target_folder_names = ['2022', '06', '30']

def lambda_handler(event, context):
    logger.info(f"event {event}")
    logger.info(f"context {context}")
    
    logger.info(f"event output: {event['params']['questions']['output']}")
    print(f"este mensaje va al default ouput (cloudwatch)")

    input_args = event['params']['questions']['input_args']
    output = event['params']['questions']['output']

    create_query(input_args, output)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def create_query(input_args, output):
    query = f"SELECT {output} FROM {athena_staging_table} WHERE {input_args} ORDER BY {output} DESC LIMIT 1"
    target_folder_path = '/'.join(target_folder_names)
    target_s3_folder = f"s3://{destination_bucket}/{athena_s3_results_folder}/{target_folder_path}/"
    
    athena_client = boto3.client('athena')
    # start_query_execution se ejecuta asyncronicamente
    athena_response = athena_client.start_query_execution(
        QueryString = query,
        QueryExecutionContext = {
            'Database': athena_staging_database
        },
        ResultConfiguration = {
            'OutputLocation': target_s3_folder
        }
    )
    
    # mirar el estado ejecucion del query para ver si la data esta lista
    execution_id = athena_response['QueryExecutionId']
    query_execution_status = athena_client.get_query_execution(QueryExecutionId=execution_id) # preguntamos cual es el estado de la ejecucion del query
    query_status = query_execution_status['QueryExecution']['Status']['State']
    logger.info(f"query_status: {query_status}")
    
    # Polling the query state
    while query_status == 'QUEUED' or query_status == 'RUNNING':
        query_execution_status = athena_client.get_query_execution(QueryExecutionId=execution_id) # preguntamos cual es el estado de la ejecucion del query
        query_status = query_execution_status['QueryExecution']['Status']['State']
        logger.info(f"query_execution_status: {query_execution_status} query_status: {query_status}")
    
    output_filename = query_execution_status['QueryExecution']['ResultConfiguration']['OutputLocation']
    logger.info(f"query: {query}, execution_id: {execution_id}, query_status: {query_status}, output_filename: {output_filename}")
    
    
    # escribir resultado en `destination_s3_bucket`/destination_folder/year/month/day
    # boto3.resource nos permite usar objetos: archivos en s3
    s3_resurce= boto3.resource('s3')
        # copiar output_filename en destination_s3_bucket 's3://devopslatam02-result/athena_results/2022/05/01/4557fef1-d80d-4e8f-b05d-aec6834df6ba.csv'
    
    output_filename_sin_s3 = output_filename.replace('s3://', '')
    tokens = output_filename_sin_s3.split('/')
    
    # TODO: improve 
    source_bucket = tokens[0]
    file_name = tokens[-1]
    source_key = output_filename.replace(f"s3://{source_bucket}", '')
    
    # athena_results/2022/05/01/4557fef1-d80d-4e8f-b05d-aec6834df6ba.csv --> publications/2022/05/01/
    target_key = source_key.replace(athena_s3_results_folder, destination_folder)
    # .replace(file_name, '')
    
    logger.info(f"trying to copy {source_bucket} {source_key} into {destination_bucket} {target_key}")
    
    # quitar el primer / en target y source key target_key.lstrip('/')
    s3_resurce.meta.client.copy( { 'Bucket': source_bucket, 'Key': source_key.lstrip('/')}, destination_bucket, target_key.lstrip('/') )