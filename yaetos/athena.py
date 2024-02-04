"""Helper functions for athena."""
import boto3
from yaetos.logger import setup_logging
logger = setup_logging('Athena')


def register_table(types, name_tb, schema, output_info, creds_or_file, is_incremental):
    # db = creds_or_file[connection_profile]
    description = 'some description'
    description_statement = f'COMMENT "{description}"' if description else ''
    output_folder = output_info['path_expanded'].replace('s3a', 's3')
    query_folder = 's3://mylake-dev/pipelines_data/athena_data/'

    types_str = ''
    # ii = 0
    ii_max = len(types) - 1
    for ii, (col_name, col_type) in enumerate(types.items()):
        # ii += 1
        types_str += f"`{col_name}` {col_type}"
        types_str += ", \n" if ii < ii_max else ''

    # import ipdb; ipdb.set_trace()
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
    else:
        raise Exception('Athena table registration not setup for other than csv files.')    

    # Start the query execution
    region_name = 'eu-west-3'
    client = boto3.client('athena', region_name=region_name)
    response = client.start_query_execution(
        QueryString=create_table_query,
        QueryExecutionContext={
            'Database': schema
        },
        ResultConfiguration={
            'OutputLocation': query_folder
        },
    )
    # logger.info(f"Registered table to athena '{schema}.{name_tb}', using connection profile '{connection_profile}', with QueryExecutionId: {response['QueryExecutionId']}")
    logger.info(f"Registered table to athena '{schema}.{name_tb}'.")

