# from pandas.testing import assert_frame_equal
# import pandas as pd
# import numpy as np
from yaetos.deploy import DeployPySparkScriptOnAws as dep


class Test_DeployPySparkScriptOnAws(object):
    def test_generate_pipeline_name(self):
        mode = 'dev_EMR'
        job_name = 'jobs.some_folder.job'
        user = 'n/a'
        actual = dep.generate_pipeline_name(mode, job_name, user)
        expected = 'yaetos__dev__jobs_d_some_folder_d_job__20220629T205103'
        assert actual[:-15] == expected[:-15]

    def test_get_job_name(self):
        pipeline_name = 'yaetos__dev__jobs_d_some_folder_d_job__20220629T205103'
        actual = dep.get_job_name(pipeline_name)
        expected = 'jobs.some_folder.job'
        assert actual == expected
