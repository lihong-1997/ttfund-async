from crawl_fund import Crawl
import time
from multiprocessing import Process
import asyncio

class App(Crawl):
    def __init__(self):
        super().__init__()
        self.data = self.tg_param()

    def run_in_new_loop_1(self, all_strategy):
        asyncio.run(self.get_all_netval(all_strategy))
    
    def run_in_new_loop_2(self, all_strategy):
        asyncio.run(self.get_all_warehouse(all_strategy))

    def run_in_new_loop_3(self, all_strategy):
        asyncio.run(self.save_tg_extend_info(all_strategy))

    def async_run(self):
        p1 = Process(target=self.run_in_new_loop_1, args=(self.data,))
        p2 = Process(target=self.run_in_new_loop_2, args=(self.data,))
        p3 = Process(target=self.run_in_new_loop_3, args=(self.data,))
        p1.start()
        p2.start()
        p3.start()
        p1.join()
        p2.join()
        p3.join()


if __name__ == "__main__":
    
    startTime = time.time()

    app = App()
    app.async_run()

    endTime = time.time()
    print(f"基金数据更新完毕，耗时 {(endTime - startTime):.2f}")