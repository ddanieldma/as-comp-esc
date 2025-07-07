from celery import Celery

# Creating main celery instance.
app = Celery(
    'scalable_computing_project',
    include=['celery_app.tasks']
)

# Loading settings
app.config_from_object('celery_app.celery_config')