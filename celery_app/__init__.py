from celery import Celery

# Creating main celery instance.
app = Celery('scalable_computing_project')

# Loading settings
app.config_from_object('celery_app.celery_config')

app.autodiscover_tasks()