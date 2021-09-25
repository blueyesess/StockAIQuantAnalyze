# -*- coding:utf-8 -*-
import pymongo
import pandas as pd

# 股票数据库类
class StockDB():
    def __init__(self):
        # token
        mongog_url = 'mongodb://localhost:27017/'

        # initialize
        self.initialize(mongog_url)

    # 初始化数据库，连接本地数据库，返回：空
    def initialize(self, mongog_url):
        mongo_client = pymongo.MongoClient(mongog_url)
        # 数据库，包含4类子数据库 basic, market, finance, ref, index，与tushare分类一致
        local_db = {
            'basic': {
                'stock_basic': mongo_client['basic']['stock_basic'],
                'trade_cal': mongo_client['basic']['trade_cal'],
                'namechange': mongo_client['basic']['namechange'],
                'hs_const': mongo_client['basic']['hs_const'],
                'stock_company': mongo_client['basic']['stock_company'],
                'stk_managers': mongo_client['basic']['stk_managers'],
                'stk_rewards': mongo_client['basic']['stk_rewards'],
                'new_share': mongo_client['basic']['new_share'],
                'bak_basic': mongo_client['basic']['bak_basic']
            },
            'market': {
                'daily': mongo_client['price_daily'],
                'weekly': mongo_client['price_weekly'],
                'monthly': mongo_client['price_monthly'],
                'pro_bar': mongo_client['market']['pro_bar'],
                'adj_factor': mongo_client['market']['adj_factor'],
                'suspend': mongo_client['market']['suspend'],
                'suspend_d': mongo_client['market']['suspend_d'],
                'moneyflow': mongo_client['market']['moneyflow'],
                'stk_limit': mongo_client['market']['stk_limit'],
                'limit_list': mongo_client['market']['limit_list'],
                'hsgt_top10': mongo_client['market']['hsgt_top10'],
                'hk_hold': mongo_client['market']['hk_hold'],
                'ggt_daily': mongo_client['market']['ggt_daily'],
                'ggt_monthly': mongo_client['market']['ggt_monthly'],
                'bak_daily': mongo_client['market']['bak_daily']
            },
            'finance': {
                'income': mongo_client['finance']['income'],
                'balancesheet': mongo_client['finance']['balancesheet'],
                'cashflow': mongo_client['finance']['cashflow'],
                'forecast': mongo_client['finance']['forecast'],
                'express': mongo_client['finance']['express'],
                'dividend': mongo_client['finance']['dividend'],
                'fina_indicator': mongo_client['finance']['fina_indicator'],
                'fina_audit': mongo_client['finance']['fina_audit'],
                'fina_mainbz': mongo_client['finance']['fina_mainbz'],
                'disclosure_date': mongo_client['finance']['disclosure_date']
            },
            'ref': {
                'ggt_top10': mongo_client['ref']['ggt_top10'],
                'margin': mongo_client['ref']['margin'],
                'margin_detail': mongo_client['ref']['margin_detail'],
                'top10_holders': mongo_client['ref']['top10_holders'],
                'top10_floatholders': mongo_client['ref']['top10_floatholders'],
                'pledge_stat': mongo_client['ref']['pledge_stat'],
                'repurchase': mongo_client['ref']['repurchase'],
                'concept': mongo_client['ref']['concept'],
                'concept_detail': mongo_client['ref']['concept_detail'],
                'share_float': mongo_client['ref']['share_float'],
                'block_trade': mongo_client['ref']['block_trade'],
                'stk_account': mongo_client['ref']['stk_account'],
                'stk_account_old': mongo_client['ref']['stk_account_old'],
                'stk_holdernumber': mongo_client['ref']['stk_holdernumber'],
                'stk_holdertrade': mongo_client['ref']['stk_holdertrade'],
                'broker_recommend': mongo_client['ref']['broker_recommend'],
                'pledge_detail': mongo_client['ref']['pledge_detail']
            },
            'index': {
                'index_basic': mongo_client['ref']['index_basic'],
                'index_daily': mongo_client['ref']['index_daily'],
                'index_weekly': mongo_client['ref']['index_weekly'],
                'index_monthly': mongo_client['ref']['index_monthly'],
                'index_weight': mongo_client['ref']['index_weight'],
                'index_dailybasic': mongo_client['ref']['index_dailybasic'],
                'index_classify': mongo_client['ref']['index_index_classify'],
                'index_member': mongo_client['ref']['index_member'],
                'daily_info': mongo_client['ref']['daily_info'],
                'sz_daily_info': mongo_client['ref']['sz_daily_info'],
                'ths_index': mongo_client['ref']['ths_index'],
                'ths_daily': mongo_client['ref']['ths_daily'],
                'ths_member': mongo_client['ref']['ths_member'],
                'index_global': mongo_client['ref']['index_global']
            }
        }

        self.__stock_db = local_db

    # 检查输入的访问项是否合规，返回：通过1， 不通过0
    # type    [string]   ：数据类型，可选basic, market, finance, ref, index
    # item    [string]   : 数据条目，每个基本类下的条目
    def is_legal(self, type, item):
        # 检查输入参数是否合规
        avail_type = ['basic', 'market', 'finance', 'ref', 'index']
        if not type in avail_type:
            print(f'Error: type "{type}" is not support, support list is {avail_type}')
            return 0
        # 检查数据库是否存在
        ret = self.__stock_db[type].get(item)
        if ret:
            return 1
        else:
            print(f'Error: item "{item}" is not found, supported item is {list(self.__stock_db[type].keys())}')
            return 0

    # 数据库插入新数据函数，返回：通过1，不通过0
    # typ     [string]   ：数据类型，可选basic, market, finance, ref, index
    # item    [string]   : 数据条目，每个基本类下的条目
    # insert  [list]     : 插入的内容, list的对象是dict键值对
    # sub     [string]   : 将数据插入子数据库
    def insert_db(self, typ, item, insert, sub):
        if self.is_legal(typ, item):
            # 如果插入内容为空
            if not insert:
                print(f'Warning: insert sub"{sub}" is None')
                return 0
            # 将所有内容转为str
            for i in range(len(insert)):
                for k,v in insert[i].items():
                    insert[i][k] = str(v)
            # 添加到数据库
            if sub:
                self.__stock_db[typ][item][sub].insert_many(insert)
            else:
                self.__stock_db[typ][item].insert_many(insert)
            return 1
        else:
            print('Error: insert db failed.')
            return 0

    # 数据库查询数据函数, 返回：通过DateFrame，不通过None
    # typ     [string]   ：数据类型，可选basic, market, finance, ref, index
    # item    [string]   : 数据条目，每个基本类下的条目
    # find_kw  [string]    : 为空时查询所有，非空时为筛选条件字典
    def query_db(self, typ, item, find_kw={}):
        if self.is_legal(typ, item):
            collection = self.__stock_db[typ][item]
            ret = pd.DataFrame(collection.find(find_kw)) if find_kw else pd.DataFrame(collection.find())
            return ret if len(ret.keys()) else None
        else:
            print("Error: query db failed.")
            return None

    # 判断指定pattern是否存在，返回true/false
    # find  [dict]      :   匹配指定键值对
    def db_exists(self, stock_id, find):
        ret = self.query_db(stock_id, find)
        return len(ret.keys()) > 0

    # 将StcokDB数据更新至最新
    def update_db(self):
        # 数据库记录时间
        initial_date = '20150101'
        api = self.query_api
        db_query = self.query_db
        db_insert = self.insert_db
        db_exists = self.db_exists
        # 获取所有股票代码
        stock_pool = api(access='basic', id='stock_basic')['ts_code']
        for stock_id in stock_pool:
            # 检查基本信息stock_basic是否存在
            filed_id = 'stock_basic'
            if not db_exists(stock_id, {'_id': filed_id}):
                ret = api(
                    access='basic',
                    id=filed_id,
                    ts_code=stock_id,
                    fields=[
                        "ts_code",
                        "symbol",
                        "name",
                        "area",
                        "industry",
                        "market",
                        "list_date",
                        "delist_date",
                        "is_hs"
                    ]
                )
                # print(ret)
                db_insert(stock_id, [{
                    '_id': filed_id,
                    'ts_code': ret['ts_code'][0],
                    'symbol': ret['symbol'][0],
                    'name': ret['name'][0],
                    'area': ret['area'][0],
                    'industry': ret['industry'][0],
                    'market': ret['market'][0],
                    'list_date': ret['list_date'][0],
                    'delist_date': ret['delist_date'][0],
                    'is_hs': ret['is_hs'][0]
                }])

            # 检查曾用名是否存在
            filed_id = 'namechange'
            if not db_exists(stock_id, {'_id': filed_id}):
                ret = api(
                    access='basic',
                    id=filed_id,
                    ts_code=stock_id
                )
                # print(ret)
                cmd = 'db_insert(stock_id, [{'f'"_id":"{filed_id}", '
                for i in range(len(ret)):
                    cmd += 'u"%s": {"start_date":"%s", "end_date":"%s", "change_reason":"%s"},' % (
                        ret['name'][i], ret['start_date'][i], ret['end_date'][i], ret['change_reason'][i])
                cmd = cmd[:-1] + '}])'
                eval(cmd)

                # }])
                # db_insert(stock_id, [{
                #     '_id':filed_id,
                #     ret[]
                # }])
                exit()


if __name__ == '__main__':
    sb = StockDB()
    # sb.insert_db('basic', 'stock_basic', [{'a': 'b', 'd': 'e'}])
    b =sb.query_db('basic', 'stock_basic')
    print(b)
    # print(sb.query_db('000001.SZ'))
# sb.update_db()
# print(sb.query_api(typ='basic', item='stock_basic'))
