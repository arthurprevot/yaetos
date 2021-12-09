from yaetos.deploy import deploy_standalone


args = {'job_param_file': 'conf/jobs_metadata.yml',
        'skip_job': True,
        'deploy': 'code'}
deploy_standalone(args)
