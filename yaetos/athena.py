"""Helper functions for athena."""
import boto3
import os
from configparser import ConfigParser
from yaetos.logger import setup_logging
logger = setup_logging('Athena')


def register_table_to_athena_catalog(types, name_tb, schema, output_info, args):
    description_statement = f"""COMMENT "{args['description']}" """ if args.get('description') else ''
    output_folder = output_info['path_expanded'].replace('s3a', 's3')

    types_str = ''
    ii_max = len(types) - 1
    for ii, (col_name, col_type) in enumerate(types.items()):
        types_str += f"`{col_name}` {col_type}"
        types_str += ", \n" if ii < ii_max else ''

    if output_info['type'] == 'csv':
        create_table_query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS `{schema}`.`{name_tb}` (
            {types_str}
            ) {description_statement}
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
            WITH SERDEPROPERTIES ('field.delim' = ',')
            STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION '{output_folder}'
            TBLPROPERTIES ('classification' = 'csv');
        """
        logger.info(f"Table registration with : \n{create_table_query} \n")
    elif output_info['type'] == 'parquet':
        create_table_query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS `{schema}`.`{name_tb}` (
            {types_str}
            ) {description_statement}
            STORED AS PARQUET
            LOCATION '{output_folder}'
        """
        logger.info(f"Table registration with : \n{create_table_query} \n")
    else:
        raise Exception('Athena table registration not setup for other than csv files.')

    # Start the query execution
    if args.get('mode') and 'dev_local' in args['mode'].split(','):
        config = ConfigParser()
        assert os.path.isfile(args.get('aws_config_file'))
        config.read(args.get('aws_config_file'))
        region_name = config.get(args.get('aws_setup'), 's3_region')
    else:
        region_name = boto3.Session().region_name

    client = boto3.client('athena', region_name=region_name)
    response = client.start_query_execution(
        QueryString=create_table_query,
        QueryExecutionContext={'Database': schema},
        ResultConfiguration={'OutputLocation': args.get('athena_out')},
    )
    logger.info(f"Registered table to athena '{schema}.{name_tb}', pointing to {output_folder}, with QueryExecutionId: {response['QueryExecutionId']}.")
    # TODO: Check to support "is_incremental"


def register_table_to_glue_catalog(schema_list, name_tb, schema, output_info, args):
    output_folder = output_info['path_expanded'].replace('s3a', 's3')

    # Start the query execution
    if args.get('mode') == 'dev_local':
        config = ConfigParser()
        assert os.path.isfile(args.get('aws_config_file'))
        config.read(args.get('aws_config_file'))
        region_name = config.get(args.get('aws_setup'), 's3_region')
    else:
        region_name = boto3.Session().region_name

    glue_client = boto3.client('glue', region_name=region_name)

    database_name = schema
    table_name = name_tb
    logger.info(f"Registering table to athena '{schema}.{name_tb}', schema {schema_list}.")

    # Define the table input
    table_input = {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': schema_list,
            'Location': output_folder,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'Compressed': False,
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {
                    'serialization.format': '1'
                }
            },
        },
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'parquet'
        }
    }

    try:
        glue_client.delete_table(DatabaseName=database_name, Name=table_name)
        print("Existing table deleted.")
    except glue_client.exceptions.EntityNotFoundException:
        print("No table to delete.")

    response = glue_client.create_table(
        DatabaseName=database_name,
        TableInput=table_input
    )
    logger.info(f"Registered table to athena '{schema}.{name_tb}', pointing to {output_folder} with query response: {response}.")
