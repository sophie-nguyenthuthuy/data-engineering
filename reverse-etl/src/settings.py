from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Postgres
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "analytics"
    postgres_user: str = "etl_user"
    postgres_password: str = ""

    # BigQuery
    bigquery_project: str = ""
    bigquery_credentials_file: str = ""

    # Snowflake
    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_password: str = ""
    snowflake_database: str = ""
    snowflake_warehouse: str = ""
    snowflake_schema: str = "PUBLIC"

    # Slack
    slack_bot_token: str = ""

    # Email
    sendgrid_api_key: str = ""
    email_from: str = "data@company.com"

    # Salesforce
    salesforce_username: str = ""
    salesforce_password: str = ""
    salesforce_security_token: str = ""
    salesforce_domain: str = "login"

    # HubSpot
    hubspot_access_token: str = ""

    # System
    log_level: str = "INFO"
    pipeline_config_dir: str = "./pipelines"
    webhook_secret: str = "changeme"
    api_host: str = "0.0.0.0"
    api_port: int = 8000


settings = Settings()
