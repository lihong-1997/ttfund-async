import functools
import time
from typing import Callable, Any
import csv


def async_timed():
    def wrapper(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapped(*args, **kwargs) -> Any:
            start = time.time()
            try:
                return await func(*args, **kwargs)
            finally:
                end = time.time()
                print(f"finished {func} in {(end - start):.4f} seconds")
        return wrapped
    return wrapper


def save_to_csv(path, **kwargs):
    with open(path, 'w', encoding='utf_8_sig', newline="") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(kwargs.keys())
        data = list(kwargs.values())
        row_num = len(data[0])
        for i in range(row_num):
            row_data = []
            for j in range(len(data)):
                row_data.append(data[j][i])
            csv_writer.writerow(row_data)
