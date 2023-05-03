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
        # advisers_code = self.get_advisers_code()
        # self.all_strategy = asyncio.run(self.get_all_strategies(advisers_code))

    def tg_param(self):
        advisers_code = self.get_advisers_code()
        all_strategy = asyncio.run(self.get_all_strategies(advisers_code))
        return all_strategy

    def get_advisers_code(self) -> list:
        """获取全部投顾公司的CODE"""
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
            res = await response.json()
            return {'name': adviser['AdvicerName'], 'tg': res}
    
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
            json_response = await response.json()
            strategy_info = []
            for item in json_response['data'][0]['sceneList']:
                strategy_list = item['strategyList']
                for strategy in strategy_list:
                    strategy_info.append(
                        {
                            'name': tg_id['name'], 
                            'tg_name': strategy['name'],
                            'code': strategy['code']
                        })
            return strategy_info

    async def get_tg_debug(self, session: aiohttp.ClientSession, tg_id: list):
        code_list = ','.join(tg_id)
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
            json_response = await response.json()
            # FIXME: 应该是这里有问题，没有弄全
            strategy_info = []
            for item in json_response['data'][0]['sceneList']:
                strategy_list = item['strategyList']
                for strategy in strategy_list:
                    strategy_info.append({'tg_name': strategy['name'],'code': strategy['code']})

            return strategy_info

    async def bug_find(self, adviser: str):
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
        code = ['1MRB2HI', '8TWSRVW', 'UJZVSUO', 'XQR30UU', 'QP9JKF4']
        async with aiohttp.ClientSession(headers=headers) as session:
            fetchers = [self.get_tg_debug(session, code)]
            for finished_task in asyncio.as_completed(fetchers):
                response = await finished_task

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
                    # if item['name'] == "民生加银基金":
                    #     print(item['tg_name'])
                    os.makedirs(folder, exist_ok=True)
                tg_name.extend(response)
        print(f"一共有 {len(tg_name)} 条策略")
        return tg_name

    async def get_tg_extend_info(self, session: aiohttp.ClientSession, tg: dict):
        url = 'https://uni-fundts.1234567.com.cn/merge/m/api/tgfund'
        fields = "ACCOUNTID,MINEXPANNSYL,INVEST_TERM,EXPECT_MAX_RETREAT,MAXEXPANNSYL,SHTIME,JJGSID,VIDEO_IMAGE_URL,ENDTIME,STARTTIME,TARGETPROFIT,TARGETANNYIELD,ANNSYL_LN,SYL_D,SHOW_TYPE,SYRQ,HEAD_DISPLAY_TYPE,HEAD_FIELD1,FIELD1_EXPLAIN,HEAD_FIELD2,FIELD2_EXPLAIN,PERFORMANCE1_DISPLAY_TYPE,BENCHMARK_INDEX,PERFORMANCE2_DISPLAY_TYPE,IS_PREVIOUS_INCOME_DISPLAY,IS_TRADE_POSITION_SHOW,QUOTA_DISPLAY,PEOPLENUM_DISPLAY,FEATURE_EXPLAIN,LABEL1,LABEL2,LABEL3,LABEL4,RECOMMEND_HOLD_TIME,STRATEGY_CONCEPT1,STRATEGY_CONCEPT2,STRATEGY_CONCEPT3,IMG_URL,JUMP_TYPE,SELECT_TYPE,KEY_TITLE1,HOLD_LIMIT1,PARTNER,TGNAME,LOGO_NAME,LOGO_URL,BUY_CFM_DAYS,ESTABDATE,RISKLEVEL,STRATEGY_RATE,SYL_Z,BENCHSYL_Z,SYL_Y,BENCHSYL_Y,SYL_3Y,BENCHSYL_3Y,SYL_6Y,BENCHSYL_6Y,SYL_1N,BENCHSYL_1N,SYL_2N,BENCHSYL_2N,SYL_3N,BENCHSYL_3N,SYL_JN,BENCHSYL_JN,SYL_LN,BENCHSYL_LN,STYPE,BUYSTATUS,SERVICE_RATE_TYPE,STEP_RATE,STRATEGY_RATE_DISCOUNT,MINBUY,STGCONCEPT,FEATURE_EXPLAIN,OPERATETIME,OPERAENDTIME,PROVISION_TYPE,PROFIT_DAYS,REDEEM_CFM_DAYS,CASH_BAG_DAYS,CASH_DAYS,COLLECT_TYPE,SERVICE_DESC,BASIC_CAL_FORMULA_REMARK,SUPPORT_RATION,RUN_STATUS,END_STATUS,MAX_RUN_DATE,ACTUAL_RUN_DATE,STATUS,SAME_FUND"
        data = {
            'appVersion': self.app_version,
            'ctoken': self.ctoken,
            'deviceid': self.device_id,
            'fields': fields,
            'pageIndex': 1,
            'pageSize': 3,
            'passportctoken': self.passport_ctoken,
            'passportid': self.passport_id,
            'passportutoken': self.passport_utoken,
            'plat': 'Iphone',
            'product': 'Fund',
            'serverversion': self.app_version,
            'tgCode': tg['code'],
            'userid': self.user_id,
            'utoken': self.utoken,
            'version': self.app_version
        }
        async with session.post(url, data=data) as response:
            res = await response.read()
            json_response = json.loads(res)
            is_valid = (json_response['data'] is not None)
            assert is_valid, f"<{tg['name']}>提供的<{tg['tg_name']}>策略暂无信息."
            row_name_list = ['策略名称', '机构', '策略理念', '成立来收益', '日涨幅', '风险等级', '建议持有', '成立天数', '业绩比较基准']
            row_value_list = []

            data = json_response['data']
            row_value_list.append(data['tgExtendInfo']['TGNAME'])
            row_value_list.append(data['tgExtendInfo']['LOGO_NAME'])
            # 处理策略理念
            info_dict = dict(data['tgExtendInfo'])
            concept1 = info_dict.get('STRATEGY_CONCEPT1')
            if concept1 is None:
                row_value_list.append(info_dict['STGCONCEPT'])
            else:
                concept_list = [concept1]
                concept2 = info_dict.get('STRATEGY_CONCEPT2')
                concept3 = info_dict.get('STRATEGY_CONCEPT3')
                if concept2 is not None:
                    concept_list.append(concept2)
                if concept3 is not None:
                    concept_list.append(concept3)
                tg_concept = '; '.join(concept_list)
                row_value_list.append(tg_concept)
            # 处理成立来收益
            row_value_list.append(data['tgExtendInfo']['SYL_LN'] + '%')
            # 处理日涨幅
            row_value_list.append(f"{data['tgExtendInfo']['SYRQ']}, ratio: {data['tgExtendInfo']['SYL_D']}%")
            # 处理风险等级
            risk_level = {
                '1': '低风险',
                '2': '中低风险',
                '3': '中风险',
                '4': '中高风险',
                '5': '高风险'
            }
            row_value_list.append(risk_level[data['tgExtendInfo']['RISKLEVEL']])
            # 处理建议持有
            if info_dict.get('RECOMMEND_HOLD_TIME') is not None:
                row_value_list.append(info_dict['RECOMMEND_HOLD_TIME'])
            else:
                row_value_list.append('--')
            # 处理成立天数
            row_value_list.append(str(data['tgExtendInfo']['continuedData']) + '天')
            # 处理业绩比较标准
            row_value_list.append(data['tgExtendInfo']['BASIC_CAL_FORMULA_REMARK'])
            # 处理特殊数据
            ch_data = {
                "user_POSITIVE_RATIO": "用户持仓正收益占比",
                "user_FGRATIO": "策略复购率",
                "user_AVGDAY": "用户平均持有时长",
                "user_STAY_RATIO": "近一年留存率",
                "excess_LN": "成立来超额收益",
                "maxretra_LN": "成立来最大回撤",
                "maxretra_1N": "近一年最大回撤",
                "mexwin_1N": "近一年跑赢基准概率",
                "profit_1Y": "持有一个月盈利概率",
                "profit_3Y": "持有三个月盈利概率",
                "profit_6Y": "持有六个月盈利概率",
                "profit_1N": "持有一年盈利概率",
            }
            tg_characteristics_page2 = dict(data['tgCharacteristics']['tgCharacteristicsPage2'])
            for key, value in tg_characteristics_page2.items():
                if key in ["user_POSITIVE_RATIO", "user_FGRATIO", "user_AVGDAY", "user_STAY_RATIO"]:
                    row_name_list.append(ch_data[key])
                    if key == "user_AVGDAY":
                        row_value_list.append(str(value) + '天')
                    else:
                        row_value_list.append(str(value) + '%')
            for key, value in tg_characteristics_page2.items():
                if key in ["excess_LN", "maxretra_LN", "maxretra_1N", "mexwin_1N"]:
                    row_name_list.append(ch_data[key])
                    row_value_list.append(str(value) + '%')
            for key, value in tg_characteristics_page2.items():
                if key in ["profit_1Y", "profit_3Y", "profit_6Y", "profit_1N"]:
                    row_name_list.append(ch_data[key])
                    row_value_list.append(str(value) + '%')

            folder = os.path.join(self.folder_name, tg['name'], tg['tg_name'])

            return {'data': {'info': row_name_list, 'value': row_value_list}, 'folder': folder}

    async def save_tg_extend_info(self, tg_name: list):
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
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
            fetchers = [self.get_tg_extend_info(session, tg) for tg in tg_name]
            for finished_task in asyncio.as_completed(fetchers):
                try:
                    response = await finished_task
                    save_to_csv(f"{response['folder']}/basic_info.csv",
                                info=response['data']['info'],
                                value=response['data']['value'],)
                except Exception as e:
                    print(f"Warn:{e}")

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

            return {'data': {'date': date, 'netVal': netVal, 'bench_netVal': bench_netVal}, 'folder': folder}

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

        # asyncio.run(self.get_all_netval(all_strategy))
        # asyncio.run(self.get_all_warehouse(all_strategy))
        # asyncio.run(self.save_tg_extend_info(all_strategy))


if __name__ == "__main__":

    startTime = time.time()
    c = Crawl()
    c.run()
    endTime = time.time()
    print(f"基金数据更新完毕，耗时 {(endTime - startTime):.2f}s")
