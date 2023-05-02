# coding:utf-8
import json
import time
import aiohttp
from queue import Queue
from get_token import GetToken
from util import async_timed, save_to_csv
import requests
import asyncio
import os
import platform


if(platform.system() == 'Windows'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class Crawl(GetToken):

    def __init__(self):
        super().__init__()
        # 存放爬取到的数据的文件名
        self.folder_name = 'DATA2'
        self.error_tgs = []
        # 连接数不限制
        self.conn = aiohttp.TCPConnector(limit=0)
        # advisers_code = self.get_advisers_code()
        # self.all_strategy = asyncio.run(self.get_all_strategies(advisers_code))

    def tg_param(self):
        advisers_code = self.get_advisers_code()
        all_strategy = asyncio.run(self.get_all_strategies(advisers_code))
        return all_strategy

    def get_advisers_code(self) -> list:
        url = 'https://appactive.1234567.com.cn/AppoperationApi/Modules/GetAdvicerHome'
        headers = {
            'Content-Type': 'application/json',
            'Referer': 'https://mpservice.com/b0381a3b634440379d330b69f09d3f8e/release/pages/selectStrategy/index',
            'clientInfo': 'ttjj-iPhone 13 Pro-iOS-iOS16.4.1',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-Hans-CN;q=1, en-CN;q=0.9, de-CN;q=0.8',
            'Connection': 'keep-alive',
            'User-Agent': 'EMProjJijin/6.6.4 (iPhone; iOS 16.4.1; Scale/3.00)',
            'GTOKEN': self.gtoken
        }
        params = {
            'AppVersion': self.app_version,
            'DeviceId': self.device_id,
            'PlatId': '1',
            'UserId': self.user_id,
            'passportid': self.passport_id,
            'product': 'EFund'
        }
        try:
            response = requests.get(url=url, headers=headers, params=params, timeout=self.timeout)
        except Exception as e:
            print("Error occurs while getting adviser code, ", e)
        else:
            json_response = response.json()
            # 解析json数据，获取机构信息列表
            adviser_list = json_response["datas"]["Modules"][1]["CustomArray"]
            # 提取机构 AdvicerCode
            # adviser_code_list = [item['AdvicerCode'] for item in adviser_list]

            return adviser_list

    async def get_strategy_id(self, session: aiohttp.ClientSession, adviser: dict):
        url = 'https://universalapi.1234567.com.cn/hxfundtrade/auxiliary/kycQuestionV2'
        params = {
            'AppType': 'ttjj',
            'MobileKey': self.device_id,
            'ctoken': self.ctoken,
            'partner': adviser['AdvicerCode'],
            'plat': 'Iphone',
            'serverversion': self.app_version,
            'userId': self.user_id,
            'utoken': self.utoken,
        }
        async with session.get(url, params=params) as response:
            res = await response.read()
            return {'name': adviser['AdvicerName'], 'tg': json.loads(res)}

    async def get_strategy_name(self, session: aiohttp.ClientSession, tg_id: dict):
        code_list = ','.join(tg_id['tg'])
        url = 'https://uni-fundts.1234567.com.cn/combine/investAdviserAggr/tgStgSceneAggrByCodeList'
        params = {
            'codeList': code_list,
            'ctoken': self.ctoken,
            'appVersion': self.app_version,
            'deviceid': self.device_id,
            'passportctoken': self.passport_ctoken,
            'passportid': self.passport_id,
            'passportutoken': self.passport_utoken,
            'plat': 'Iphone',
            'product': 'Fund',
            'serverversion': self.app_version,
            'userid': self.user_id,
            'utoken': self.utoken,
            'version': self.app_version
        }
        async with session.get(url, params=params) as response:
            res = await response.read()
            json_response = json.loads(res)
            strategy_info = [
                {
                    'name': tg_id['name'],
                    'tg_name': item['strategyList'][0]['name'],
                    'code': item['strategyList'][0]['code']
                }
                for item in json_response['data'][0]['sceneList']
            ]
            return strategy_info

    
    async def get_all_strategies(self, advisers_code: list):
        q = Queue()
        headers = {
            'Content-Type': 'application/json',
            'Referer': 'https://mpservice.com/funda91a99886abf7e/release/pages/question/index',
            'clientInfo': 'ttjj-iPhone 13 Pro-iOS-iOS16.4.1',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-Hans-CN;q=1, en-CN;q=0.9, de-CN;q=0.8',
            'Connection': 'keep-alive',
            'User-Agent': 'EMProjJijin/6.6.4 (iPhone; iOS 16.4.1; Scale/3.00)',
            'GTOKEN': self.gtoken
        }
        async with aiohttp.ClientSession(headers=headers) as session:
            fetchers = [self.get_strategy_id(session, code) for code in advisers_code]
            for finished_task in asyncio.as_completed(fetchers):
                response = await finished_task
                q.put(response)
        tg_list = []
        while q.qsize():
            json_response = q.get()
            # 获取投资需求问卷列表
            data = json_response['tg']["data"]
            # 获取策略ID
            strategy_info = data["ruleList"]
            # 解析为单个策略ID
            strategyId_list = []
            for strategy in strategy_info:
                id_list = strategy['strategyId']
                strategyId_list.extend(id_list.split('&'))
            # 去除部分策略ID包含无效字符'*"的情况
            strategyId_list = [strategyId.strip('*') for strategyId in strategyId_list]
            # 去除策略ID为'0'的情况
            strategyId_list = [ID for ID in strategyId_list if ID != '0']
            # 去除重复项
            strategyId_list = list(set(strategyId_list))
            tg_info = {'name': json_response['name'], 'tg': strategyId_list}
            tg_list.append(tg_info)
        tg_name = []
        async with aiohttp.ClientSession(headers=headers) as session:
            fetchers = [self.get_strategy_name(session, tg) for tg in tg_list]
            for finished_task in asyncio.as_completed(fetchers):
                response = await finished_task
                for item in response:
                    folder = os.path.join(self.folder_name, item['name'], item['tg_name'])
                    os.makedirs(folder, exist_ok=True)
                tg_name.extend(response)
        return tg_name

    async def get_all_netval(self, tg_name: list):
        headers = {
            'Content-Type': 'application/json',
            'Referer': 'https://mpservice.com/funda91a99886abf7e/release/pages/strategyDetail/index',
            'clientInfo': 'ttjj-iPhone 13 Pro-iOS-iOS16.4.1',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-Hans-CN;q=1, en-CN;q=0.9, de-CN;q=0.8',
            'Connection': 'keep-alive',
            'User-Agent': 'EMProjJijin/6.6.4 (iPhone; iOS 16.4.1; Scale/3.00)',
            'GTOKEN': self.gtoken
        }
        async with aiohttp.ClientSession(headers=headers) as session:
            fetchers = [self.get_netval(session, tg) for tg in tg_name]
            for finished_task in asyncio.as_completed(fetchers):
                try:
                    response = await finished_task
                    save_to_csv(f"{response['folder']}/netval.csv",
                                date=response['data']['date'],
                                SE=response['data']['netVal'],
                                BENCH_SE=response['data']['bench_netVal'])
                except Exception as e:
                    print(f"Warn:{e}")

    async def get_all_warehouse(self, tg_name: list):
        headers = {
            'Content-Type': 'application/json',
            'Referer': 'https://mpservice.com/funda91a99886abf7e/release/pages/positionHistory/index',
            'clientInfo': 'ttjj-iPhone 13 Pro-iOS-iOS16.4.1',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-Hans-CN;q=1, en-CN;q=0.9, de-CN;q=0.8',
            'Connection': 'keep-alive',
            'User-Agent': 'EMProjJijin/6.6.4 (iPhone; iOS 16.4.1; Scale/3.00)',
            'GTOKEN': self.gtoken
        }
        async with aiohttp.ClientSession(headers=headers) as session:
            fetchers = [self.get_warehouse(session, tg) for tg in tg_name]
            for finished_task in asyncio.as_completed(fetchers):
                try:
                    response = await finished_task
                    save_to_csv(f"{response['folder']}/warehouse.csv",
                                date=response['data']['date'],
                                type=response['data']['fund_type'],
                                totalratio=response['data']['ratio'],
                                fundlist=response['data']['fund_data'])
                except Exception as e:
                    print(f"Warn:{e}")
                # TODO: 确定异常的类型

    async def get_netval(self, session: aiohttp.ClientSession, tg: dict):

        url = 'https://uni-fundts.1234567.com.cn/dataapi/IATG/FundIAAIChart'
        params = {
            'CODE': tg['code'],
            'RANGE': 'ln',
            'ctoken': self.ctoken,
            'appVersion': self.app_version,
            'deviceid': self.device_id,
            'passportctoken': self.passport_ctoken,
            'passportid': self.passport_id,
            'passportutoken': self.passport_utoken,
            'plat': 'Iphone',
            'product': 'Fund',
            'serverversion': self.app_version,
            'userid': self.user_id,
            'utoken': self.utoken,
            'version': self.app_version
        }
        async with session.get(url, params=params) as response:
            res = await response.read()
            json_response = json.loads(res)
            # 判断数据是否为空
            is_valid = (json_response['data'] is not None)
            assert is_valid, f"<{tg['name']}>提供的<{tg['tg_name']}>策略暂无净值数据."
            date = []
            netVal = []
            bench_netVal = []
            folder = os.path.join(self.folder_name, tg['name'], tg['tg_name'])
            for item in json_response['data']:
                date.append(item['PDATE'])
                netVal.append(item['SE'] + '%')
                bench_netVal.append(item['BENCH_SE'] + '%')

            return {'is_valid': is_valid, 'data': {'date': date, 'netVal': netVal, 'bench_netVal': bench_netVal}, 'folder': folder}

    async def get_warehouse(self, session: aiohttp.ClientSession, tg: dict):

        url = 'https://uni-fundts.1234567.com.cn/combine/investAdviserInfo/getAdjustWarehouse'
        params = {
            'tgCode': tg['code'],
            'RANGE': 'ln',
            'ctoken': self.ctoken,
            'appVersion': self.app_version,
            'deviceid': self.device_id,
            'passportctoken': self.passport_ctoken,
            'passportid': self.passport_id,
            'passportutoken': self.passport_utoken,
            'plat': 'Iphone',
            'product': 'Fund',
            'serverversion': self.app_version,
            'tag': '1',
            'userid': self.user_id,
            'utoken': self.utoken,
            'version': self.app_version
        }
        async with session.get(url, params=params) as response:
            res = await response.read()
            json_response = json.loads(res)
            # 判断是否有数据
            is_valid = ('adjustHistory' in dict(json_response['Data'])) and (json_response['Data']['adjustHistory'] is not None) and (len(json_response['Data']['adjustHistory']) > 0)
            assert is_valid, f"<{tg['name']}>提供的<{tg['tg_name']}>策略暂无历史调仓数据."
            # 解析json
            date = []
            ratio = []
            fund_type = []
            classify_fund = []
            folder = os.path.join(self.folder_name, tg['name'], tg['tg_name'])
            type_map = {
                '0': '其他',
                '1': '股票型',
                '2': '货币型',
                '3': '混合型',
                '4': '其他',
                '6': '债券型',
                '7': '保本型',
                '8': '指数型',
                'a': 'QDII'}
            for item in json_response['Data']['adjustHistory']:
                adjust_list = item['adjustList']
                for i, adjust_info in enumerate(adjust_list):
                    temp_dict = dict()
                    fund_list = adjust_info['fundList']
                    for fund in fund_list:
                        temp_dict[fund['fundCode']] = fund['afterRatio'] + '%'
                    date.append(item['dateStr'] if i == 0 else "")
                    fund_type.append(type_map[adjust_info['type']])
                    ratio.append(adjust_info['afterTotalRatio'] + '%')
                    classify_fund.append(temp_dict)
            warehouse_data = {
                'data': {
                    'date': date,
                    'fund_type': fund_type,
                    'ratio': ratio,
                    'fund_data': classify_fund},
                'folder': folder}
            return warehouse_data

    def run(self):

        advisers_code = self.get_advisers_code()
        all_strategy = asyncio.run(self.get_all_strategies(advisers_code))

        asyncio.run(self.get_all_netval(all_strategy))
        asyncio.run(self.get_all_warehouse(all_strategy))

    def run_in_new_loop_1(self, all_strategy):
        asyncio.run(self.get_all_netval(all_strategy))
    
    def run_in_new_loop_2(self, all_strategy):
        asyncio.run(self.get_all_warehouse(all_strategy))


# startTime = time.time()
# c = Crawl()
# c.run()
# endTime = time.time()
# print("基金数据更新完毕", endTime - startTime)
