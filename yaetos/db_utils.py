"""Helper functions for databases, based on sqlalchemy, mostly for oracle for now.
Some code to be run in worker nodes, so can't rely on libraries only on master (ex db connections)."""

import pandas as pd
from sqlalchemy import types as db_types
from pyspark.sql import types as spk_types
from datetime import datetime, date
from yaetos.logger import setup_logging
logger = setup_logging('DB_Utils')


def cast_rec(rec, output_types):
    new_rec = {}
    for field in output_types.keys():
        new_rec[field] = cast_value(rec[field], output_types[field], field)
    return new_rec


def cast_value(value, required_type, field_name):
    # TODO: make it less ugly.. or avoid using pandas to not require this.
    try:
        if isinstance(required_type, type(db_types.DATE())):
            if isinstance(value, str):
                return datetime.strptime(value, "%Y-%m-%d")  # assuming iso format
            elif isinstance(value, pd.Timestamp):  # == datetime
                return value.to_pydatetime().date()
            elif isinstance(value, date):
                return value
            elif pd.isnull(value):
                return None
            else:
                return required_type.python_type(value)
        if isinstance(required_type, type(db_types.DATETIME())):
            if isinstance(value, str):
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")  # assuming iso format
            elif isinstance(value, pd.Timestamp):
                return value.to_pydatetime()
            elif pd.isnull(value):
                return None
            else:
                return required_type.python_type(value)
        elif isinstance(required_type, type(db_types.VARCHAR())):
            return None if pd.isnull(value) else str(value)
        elif isinstance(required_type, type(db_types.INT())):
            return None if pd.isnull(value) else int(float(value))
        elif isinstance(required_type, type(db_types.BIGINT())):
            return None if pd.isnull(value) else int(value)
        elif isinstance(required_type, type(db_types.FLOAT())):
            return None if pd.isnull(value) else float(value)
        else:
            return required_type.python_type(value)
    except Exception as e:
        logger.error(u"cast_value issue: {}, {}, {}, {}, {}.".format(field_name, value, type(value), required_type, str(e)))
        return None


def cast_col(df, output_types):
    for field in df.columns:
        if isinstance(output_types[field], type(db_types.FLOAT())):
            df[field] = df[field].astype(float)
        if isinstance(output_types[field], type(db_types.DATE())):
            df[field] = df[field].astype('datetime64[ns]')
        # if isinstance(output_types[field], type(db_types.INT())):
        #     df[field] = df[field].astype(int)  -> this "if" leads to pb in prod when they are none value + handled properly in prod with string inputs being converted to int.
    return df


def get_spark_type(field, required_type):
    if isinstance(required_type, type(db_types.DATE())):
        return spk_types.StructField(field, spk_types.DateType(), True)
    elif isinstance(required_type, type(db_types.DATETIME())):
        return spk_types.StructField(field, spk_types.TimestampType(), True)
    elif isinstance(required_type, type(db_types.VARCHAR())):
        return spk_types.StructField(field, spk_types.StringType(), True)
    elif isinstance(required_type, type(db_types.INT())):
        return spk_types.StructField(field, spk_types.LongType(), True)  # db type enforced earlier than spark ones, so spark types needs to be less restrictive than spark ones so needs to choose LongType instead of IntegerType
    elif isinstance(required_type, type(db_types.FLOAT())):
        return spk_types.StructField(field, spk_types.FloatType(), True)
    elif isinstance(required_type, type(db_types.BOOLEAN())):
        return spk_types.StructField(field, spk_types.BooleanType(), True)
    else:
        raise Exception("Type not recognized, field={}, required_type={}".format(field, required_type))
    # BIGINT not usable here as not supported by Oracle.


def get_spark_types(output_types):
    spark_types = []
    for field, required_type in output_types.items():
        spark_type = get_spark_type(field, required_type)
        spark_types.append(spark_type)

    spark_schema = spk_types.StructType(spark_types)
    logger.info('spark_schema: {}'.format(spark_schema))
    return spark_schema


def pdf_to_sdf(df, output_types, sc, sc_sql):  # TODO: check suspicion that this leads to each node requiring loading all libs from this script.
    # can now be done with sdf = self.sc_sql.createDataFrame(pdf). TODO: check to deprecate pdf_to_sdf if createDataFrame(pdf) covers every cases.
    spark_schema = get_spark_types(output_types)
    missing_columns = set(df.columns) - set(output_types.keys())
    if missing_columns:
        logger.warning('Some fields from source pandas df will not be pushed to spark df (because of absence in output_types), check if need to be added: {}.'.format(missing_columns))

    recs = df.to_dict(orient='records')
    partitions = len(recs) / 1000
    partitions = partitions if partitions >= 1 else None
    # Push to spark. For easier testing of downstream casting (i.e. outside of spark): tmp = [cast_rec(row, output_types) for row in recs]
    rdd = sc.parallelize(recs, numSlices=partitions) \
            .map(lambda row: cast_rec(row, output_types))

    return sc_sql.createDataFrame(rdd, schema=spark_schema, verifySchema=True)


def pandas_types_to_hive_types(df):
    """
    Converts pandas DataFrame dtypes to Hive column types.

    :param df: pandas DataFrame
    :return: Dictionary of column names and their Hive data types
    """
    type_mapping = {
        'object': 'STRING',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
        'timedelta[ns]': 'STRING',  # Hive does not have a direct equivalent
        'category': 'STRING',  # Hive has no direct category type; consider using STRING or a specific type based on the category

        # Integer types
        'int8': 'TINYINT',
        'int16': 'SMALLINT',
        'int32': 'INT',
        'int64': 'BIGINT',
        'uint8': 'SMALLINT',  # Hive does not have unsigned types, so use the next larger type
        'uint16': 'INT',
        'uint32': 'BIGINT',
        'uint64': 'BIGINT',  # Note: Hive BIGINT might not cover the full range of uint64

        # Floating types
        'float16': 'FLOAT',
        'float32': 'FLOAT',
        'float64': 'DOUBLE',

        # Special handling for decimals
        # This is a placeholder; actual handling should consider the specific precision and scale
        # 'decimal': 'DECIMAL',
    }
    hive_types = {}
    for column, dtype in df.dtypes.items():
        dtype_name = dtype.name
        hive_type = type_mapping.get(dtype_name, 'STRING')  # Default to STRING if no mapping found
        hive_types[column] = hive_type
    return hive_types


def spark_type_to_hive_type(data_type):
    """ Convert Spark data types to a detailed readable string format, handling nested structures. """
    from pyspark.sql.types import StructType, ArrayType, FloatType, DecimalType, TimestampType  # StructField, IntegerType, StringType, 

    if isinstance(data_type, StructType):
        # Handle nested struct by recursively processing each field
        fields = [f"{field.name}: {spark_types_to_hive_types(field.dataType)}" for field in data_type.fields]
        return f"struct<{', '.join(fields)}>"
    elif isinstance(data_type, ArrayType):
        # Handle arrays by describing element types
        element_type = spark_types_to_hive_types(data_type.elementType)
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


def spark_types_to_hive_types(sdf):
    schema_list = [{"Name": field.name, "Type": spark_type_to_hive_type(field.dataType)} for field in sdf.schema]
    return schema_list
