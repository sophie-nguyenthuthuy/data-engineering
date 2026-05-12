import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://superset:{os.environ['SUPERSET_DB_PASSWORD']}"
    f"@postgres:5432/superset"
)

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ALERT_REPORTS": False,
}

ENABLE_PROXY_FIX = True
WEBDRIVER_BASEURL = "http://superset:8088/"
SQLLAB_CTAS_NO_LIMIT = True
