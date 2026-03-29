import time
from main import run_crawler

print('Khởi động Weather Crawler...')
while True:
    run_crawler()
    print('Vừa crawl thành công')
    time.sleep(10)
