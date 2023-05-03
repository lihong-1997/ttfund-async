import os
file_path = "/root/ttfund-async/DATA2"
dir_count = 0
file_count = 0
for root, dirs, filenames in os.walk(file_path):
    for _ in dirs:
        dir_count += 1
    for _ in filenames:
        file_count += 1
print('dir_count ', dir_count)
print('file_count ', file_count)

for partner in os.listdir('/root/ttfund-async/DATA2'):
    cnt = 0
    for tg in os.listdir(os.path.join('/root/ttfund-async/DATA2', partner)):
        cnt = cnt + 1
        num = 0
        for csv in os.listdir(os.path.join('/root/ttfund-async/DATA2', partner, tg)):
            num = num + 1
        if num < 3 or num > 3:
            print(partner, tg, "less or more data")
    print(partner, cnt)


# 299 - 29 = 270 策略数量
# 270 * 3 - 2 = 808 文件数量