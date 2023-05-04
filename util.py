import functools
import time
from typing import Callable, Any
import csv
import aiofiles
from aiocsv import AsyncWriter


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
        data = list(zip(*data))
        csv_writer.writerows(data)


async def async_save_to_csv(path, **kwargs):
    async with aiofiles.open(path, 'w', encoding='utf_8_sig', newline="") as f:
        csv_writer = AsyncWriter(f)
        await csv_writer.writerow(kwargs.keys())
        data = list(kwargs.values())
        data = list(zip(*data))
        await csv_writer.writerows(data)
