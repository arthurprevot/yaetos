"""Helper functions for athena."""
import boto3
import os
from configparser import ConfigParser
from yaetos.logger import setup_logging
logger = setup_logging('Athena')


def register_table(types, name_tb, schema, output_info, args):
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
    logger.info(f"Registered table to athena '{schema}.{name_tb}', with QueryExecutionId: {response['QueryExecutionId']}.")
    # TODO: Check to support "is_incremental"


def register_table_from_sdf_to_glue(df, name_tb, schema, output_info, args):
    output_folder = output_info['path_expanded'].replace('s3a', 's3')

    # Start the query execution
    if args.get('mode') == 'dev_local':
        config = ConfigParser()
        assert os.path.isfile(args.get('aws_config_file'))
        config.read(args.get('aws_config_file'))
        region_name = config.get(args.get('aws_setup'), 's3_region')
    else:
        region_name = boto3.Session().region_name
    
    # glue_client = boto3.client('glue')
    glue_client = boto3.client('glue', region_name=region_name)

    database_name = schema  # The Glue database to add the table to
    table_name = name_tb        # The new table name
    schema_list = [{"Name": field.name, "Type": convert_data_type(field.dataType)} for field in df.schema]
    logger.info(f"Registering table to athena '{schema}.{name_tb}', schema {schema_list}.")

    # Define the table input
    table_input = {
        'Name': table_name,
        'StorageDescriptor': {
            # 'Columns': [
            #     {'Name': 'name', 'Type': 'string'},
            #     {'Name': 'age', 'Type': 'int'}
            # ],
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
        # Try deleting the table first
        glue_client.delete_table(DatabaseName=database_name, Name=table_name)
        print("Existing table deleted.")
    except glue_client.exceptions.EntityNotFoundException:
        print("No table to delete.")

    # Create or update the table in Glue Catalog
    response = glue_client.create_table(
        DatabaseName=database_name,
        TableInput=table_input
    )
    # logger.info(f"Registered table to athena '{schema}.{name_tb}', with QueryExecutionId: {response['QueryExecutionId']}.")
    logger.info(f"Registered table to athena '{schema}.{name_tb}', with query response: {response}.")


def convert_data_type(data_type):
    """ Convert Spark data types to a detailed readable string format, handling nested structures. """
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType, DecimalType, TimestampType

    if isinstance(data_type, StructType):
        # Handle nested struct by recursively processing each field
        fields = [f"{field.name}: {convert_data_type(field.dataType)}" for field in data_type.fields]
        return f"struct<{', '.join(fields)}>"
    elif isinstance(data_type, ArrayType):
        # Handle arrays by describing element types
        element_type = convert_data_type(data_type.elementType)
        return f"array<{element_type}>"
    elif isinstance(data_type, TimestampType):
        return "timestamp"
    elif isinstance(data_type, FloatType):
        return "float"
    elif isinstance(data_type, DecimalType):
        return f"decimal({data_type.precision},{data_type.scale})"
    else:
        # Fallback for other types with default string representation
        return data_type.simpleString()