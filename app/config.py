from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str = "sqlite+aiosqlite:///./yakjosim.db"
    dur_api_key: str = ""
    clova_ocr_url: str = ""
    clova_ocr_secret: str = ""
    anthropic_api_key: str = ""
    environment: str = "development"

    model_config = {"env_file": ".env", "extra": "ignore"}

settings = Settings()
