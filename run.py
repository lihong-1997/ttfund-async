from crawl_fund import Crawl
import time
from multiprocessing import Process

class App:
    def __init__(self):
        self.c = Crawl()
        self.data = self.c.tg_param()

    def async_run(self):
        p1 = Process(target=self.c.run_in_new_loop_1, args=(self.data,))
        p2 = Process(target=self.c.run_in_new_loop_2, args=(self.data,))
        p1.start()
        p2.start()
        p1.join()
        p2.join()


if __name__ == "__main__":
    
    startTime = time.time()

    app = App()
    app.async_run()

    endTime = time.time()
    print("基金数据更新完毕", endTime - startTime)