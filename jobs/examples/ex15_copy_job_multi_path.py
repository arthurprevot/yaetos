from yaetos.etl_utils import ETL_Base, Commandliner, Path_Handler


class Job(ETL_Base):

    def transform(self, table_to_copy):
        for category in self.jargs.category:
            for subcategory in self.jargs.subcategory:
                self.logger.info(f"--- About to process category={category}, subcategory={subcategory} ---")

                # params
                name = 'table_to_copy'
                path_raw = self.jargs.inputs['table_to_copy']['path']
                type = self.jargs.inputs['table_to_copy']['type']
                sc, sc_sql = None, None 
                df_meta = self.jargs.inputs['table_to_copy']
                kwargs = {}
                kwargs['category'] = category
                kwargs['subcategory'] = subcategory
                path_raw_out = self.jargs.output['path']
                base_path = self.jargs.base_path
                type_out = self.jargs.output['type']

                # transfo
                df_in = self.load_data_from_files(name, path_raw, type, sc, sc_sql, df_meta, **kwargs)
                df_out = self.transform_one(df_in)
                self.save(df_out, path_raw_out, base_path, type_out, now_dt=self.start_dt, is_incremental=None, incremental_type=None, partitionby=None, file_tag=None, **kwargs)

        return None

    def transform_one(self, table_to_copy):
        if self.jargs.output.get('df_type', 'spark') == 'spark':
            table_to_copy.cache()
            if table_to_copy.count() < 500000:
                table_to_copy = table_to_copy.repartition(1)
        return table_to_copy

    def expand_input_path(self, path, **kwargs):
        category = kwargs['category']
        subcategory = kwargs['subcategory']
        base_path = self.jargs.base_path
        path_partly_expanded = path.replace('{category}', category) \
                                   .replace('{subcategory}', subcategory)
        path = Path_Handler(path_partly_expanded, base_path, self.jargs.merged_args.get('root_path')).expand_later()
        return path

    def expand_output_path(self, path, now_dt, **kwargs):
        category = kwargs['category']
        subcategory = kwargs['subcategory']
        # base_path_expanded = self.jargs.base_path.replace('{region}', region)
        # base_path_expanded = self.jargs.base_path.replace('{region}', region)
        base_path = self.jargs.base_path
        path_partly_expanded = path.replace('{category}', category) \
                                   .replace('{subcategory}', subcategory)
        path = Path_Handler(path_partly_expanded, base_path, self.jargs.merged_args.get('root_path')).expand_now(now_dt)
        return path


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
