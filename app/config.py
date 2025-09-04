from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import List

class Settings(BaseSettings):
    redis_url: str = Field("redis://chris:Robert@2006@redis-18998.c80.us-east-1-2.ec2.redns.redis-cloud.com:18998", env="REDIS_URL")
    file_upload_max_size: int = Field(100_000_000, env="FILE_UPLOAD_MAX_SIZE")
    allowed_file_extensions: str = Field("csv,json,xlsx,txt", env="ALLOWED_FILE_EXTENSIONS")
    temp_file_expiry: int = Field(3600, env="TEMP_FILE_EXPIRY")

    @property
    def allowed_extensions_list(self) -> List[str]:
        return [ext.strip() for ext in self.allowed_file_extensions.split(",")]

    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()
