from functools import lru_cache

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    fred_api_key: SecretStr | None = None
    kafka_bootstrap_servers: str = "localhost:9092"

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", env_ignore_empty=True, extra="ignore"
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()


def main() -> None:
    settings = get_settings()
    print(f"Kafka: {settings.kafka_bootstrap_servers}")
    print(f"FRED key configured: {settings.fred_api_key is not None}")


if __name__ == "__main__":
    main()
