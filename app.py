from helpers.sentry import is_sentry_enabled, initialize_sentry
from celery import Celery

import celery_config

if is_sentry_enabled():
    initialize_sentry()


celery_app = Celery("tasks")
celery_app.config_from_object(celery_config)
