from dotenv import load_dotenv

from pathlib import Path
import os

load_dotenv()
file_path = Path('.')/'.env'
print(file_path)
load_dotenv(dotenv_path = file_path)

base_url = os.getenv("BASE_URL")
access_token = os.getenv("ACCESS_TOKEN")
