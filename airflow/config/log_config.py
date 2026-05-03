from copy import deepcopy

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG


LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)

# Avoid file-backed processor manager logs on local Docker/macOS setups where
# the rotating handler can raise Errno 35 during scheduler stat logging.
LOGGING_CONFIG["handlers"]["processor_manager_stdout"] = {
    "class": "airflow.utils.log.logging_mixin.RedirectStdHandler",
    "formatter": "airflow",
    "stream": "sys.stdout",
    "filters": ["mask_secrets"],
}

LOGGING_CONFIG["loggers"]["airflow.processor_manager"] = {
    "handlers": ["processor_manager_stdout"],
    "level": LOGGING_CONFIG["root"]["level"],
    "propagate": False,
}
