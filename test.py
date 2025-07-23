import os

print(os.getcwd())
from datetime import datetime

base_path = os.getcwd()
today_date = datetime.now().strftime("%Y-%m-%d")
print(today_date)