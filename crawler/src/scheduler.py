import time
from weather_crawler import run_crawler


while run_crawler():
    time.sleep(1)
