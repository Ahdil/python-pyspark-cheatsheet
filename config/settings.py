import os
import urllib.parse
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


class Settings:
    JSON_MOCK_DATA_BASIC_URL = "https://jsonplaceholder.typicode.com/"
    JSON_ALPHAVANTAGE_BASIC_URL = "https://www.alphavantage.co/query"


@dataclass
class AzureConfig:
    user: str = field(default_factory=lambda: os.getenv("AZURE_USER"))
    password: str = field(default_factory=lambda: os.getenv("AZURE_PASSWORD"))
    server: str = field(default_factory=lambda: os.getenv("AZURE_SERVER_ENDPOINT"))
    database: str = field(default_factory=lambda: os.getenv("AZURE_DB"))
    port: str = "1433"

    @property
    def connection_url(self) -> str:
        if not all([self.user, self.password, self.server, self.database]):
            raise ValueError("Invalid configuration in .env file")
        return f"""jdbc:sqlserver://{self.server}.database.windows.net:{self.port};
                    database={self.database};
                    user={self.user};
                    password={self.password};
                    encrypt=true;
                    trustServerCertificate=false;
                    hostNameInCertificate=*.database.windows.net;
                    loginTimeout=30;
                    """

# Not used yet, Not tested
@dataclass
class AWSConfig:
    user: str = field(default_factory=lambda: os.getenv("AWS_USER"))
    password: str = field(default_factory=lambda: os.getenv("AWS_PASSWORD"))
    host: str = field(default_factory=lambda: os.getenv("AWS_HOST"))
    database: str = field(default_factory=lambda: os.getenv("AWS_DB"))
    port: str = "5432"

    @property
    def connection_url(self) -> str:
        if not all([self.user, self.password, self.host, self.database]):
            raise ValueError("Invalid configuration in .env file")
        encoded_user = urllib.parse.quote_plus(self.user)
        encoded_password = urllib.parse.quote_plus(self.password)
        return f"postgresql+psycopg2://{encoded_user}:{encoded_password}@{self.host}:{self.port}/{self.database}"
