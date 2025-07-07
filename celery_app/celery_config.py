broker_url = 'redis://redis:6379/0'
result_backend = 'redis://redis:6379/1'

task_serializer = 'pickle'
result_serializer = 'pickle'
accept_content = ['pickle', 'json']

timezone = 'America/Sao_Paulo'
enable_utc = True

task_acks_late = True

worker_prefetch_multiplier = 1