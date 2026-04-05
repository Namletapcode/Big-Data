import time
from main import run_crawler


print('Khởi động Weather Crawler...')

while run_crawler():
    print('Vừa crawl thành công')
    time.sleep(1)
