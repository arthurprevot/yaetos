import pandas as pd
from yaetos.etl_utils import Path_Handler
from yaetos.db_utils import pdf_to_sdf
from yaetos.logger import setup_logging
logger = setup_logging('Job')


def load_excel(jargs, input_name, output_types, sc, sc_sql, **xls_args):
    pdf = load_excel_pandas(jargs, input_name, **xls_args)
    sdf = pdf_to_sdf(pdf, output_types, sc, sc_sql)
    return sdf


def load_excel_pandas(jargs, input_name, **xls_args):

    path = jargs.inputs[input_name]['path']
    path = path.replace('s3://', 's3a://') if jargs.mode == 'dev_local' else path
    logger.info("Input '{}' to be loaded from files '{}'.".format(input_name, path))
    path = Path_Handler(path, jargs.base_path).expand_later()
    logger.info("Input '{}' loaded from files '{}'.".format(input_name, path))

    pdf = pd.read_excel(io=path, engine='openpyxl', **xls_args)
    return pdf
