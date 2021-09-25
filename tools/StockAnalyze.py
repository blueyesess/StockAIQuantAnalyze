# -*- coding:utf-8 -*-
from StockDB import *
from DataSource import *
from StockTool import *

# 股票分析量化类
class StockAnalyze:
    def __init__(self):
        # 上次执行进度
        self.last_process = 0
        # 跳过的item，将不会查询api
        self.skip_lis = ['stock_basic', 'namechange', 'trade_cal', 'stock_company', 'new_share', 'daily', 'weekly',
                         'monthly']
        # 使用sub数据库的item，将以股票代码作为collection的tag
        self.sub_lis = ['daily', 'weekly', 'monthly']
        # 需要每天更新的item
        self.daily_update_lis = []
        # 有使用次数的item，到达使用次数后，忽略api异常
        day_limit = ['bak_basic']
        # 有每分钟访问次数限制的item
        min_limit = ['weekly', 'monthly']

        self.db = StockDB()
        self.ds = DataSource(day_limit=day_limit, min_limit=min_limit);

    # 将api的所有数据更新至指定日期
    def update_data(self, start_date='', end_date=''):
        ds = self.ds
        for typ, items in ds.id_dic.items():  # 五个基本类
            for itm in items.keys():  # 每个里的小类
                # 跳过的item
                if itm in self.skip_lis:
                    continue
                # print(typ, itm)
                # api收数据并转为对应字典
                data_dic_list, sub = self.__data_process(typ, itm)
                # print(data_dic_list)
                # 当api读取失败时，跳过当前item
                if data_dic_list[0] == {}:
                    continue
                else:
                    self.__data_insert(typ, itm, data_dic_list, sub)

    # 将访问api，并将返回的数据转换为字典，后续将写入数据库
    # typ   [string]    :   访问的数据类型
    # itm   [string]    :   访问的数据子类
    def __data_process(self, typ, itm):
        # 后处理，将df转换成字典格式
        def post_process(typ, itm, ret):
            # 当api读取失败时，返回None
            if ret is None:
                print(f'Warning: skip typ "{typ}" and itm "{itm}"')
                return {}, None
            data_dic_list = []
            for i in range(len(ret)):
                # 将df转换为dict
                data_list = ret.to_dict(orient='records')
                # 若为sub数据库，将二维数据重新排版
                if sub:
                    for dic in data_list:
                        dic['_id'] = dic['trade_date']
                        data_dic_list.append(dic)
                    return data_dic_list
                else:
                    for dic in data_list:
                        # 将股票代码作为_id
                        tag = list(dic.keys())
                        # 将不同数据类型用不同方式存储
                        if 'ts_code' in tag:
                            dic['_id'] = dic['ts_code'].split('.')[0]
                        elif 'cal_date' in tag:
                            dic['_id'] = dic['cal_date']
                        else:
                            print('Bug: can not find tag.')
                            print(typ, itm)
                            exit()
                        data_dic_list.append(dic)
                    return data_dic_list

        ds = self.ds
        # 判断sub是否需要赋值
        sub = True if itm in self.sub_lis else False
        # 如果是sub模式，将以股票池的形式访问api
        if sub:
            data_dic_list = []
            total_process = len(ds.stock_pool)
            cur_process = 0
            max_item = 10
            for stock in ds.stock_pool:
                # print(stock)
                if bar(cur_process, total_process, last_process=self.last_process):
                    ret = ds.query_api(access=typ, id=itm, ts_code=stock)
                    # print(typ, itm, stock)
                    sub_dic_list = post_process(typ, itm, ret)
                    data_dic_list.append({stock: sub_dic_list})
                    cur_process += 1
                else:
                    cur_process += 1
                    continue
                # 当数据超过限制时，将当前数据写入数据库
                if cur_process % max_item == 0:
                    self.__data_insert(typ, itm, data_dic_list, sub)
                    data_dic_list = []
            print()
        else:
            ret = ds.query_api(access=typ, id=itm)
            data_dic_list = post_process(typ, itm, ret)
        return data_dic_list, sub

    # 将字典型数组插入数据库，返回None
    # typ               [string]    :   访问的数据类型
    # itm               [string]    :   访问的数据子类
    # data_dic_list     [dict]      :   存放字典的数组
    # sub               [sting]     :   sub模块名，非空即视为调用sub数据库
    def __data_insert(self, typ, itm, data_dic_list, sub):
        db = self.db
        # 写入数据库
        try:
            # 调用sub数据库
            if sub:
                for lis in data_dic_list:
                    stock_id = list(lis.keys())[0]
                    db.insert_db(typ, itm, lis[stock_id], stock_id)
            else:
                db.insert_db(typ, itm, data_dic_list, sub)
        except pymongo.errors.BulkWriteError:
            print(f'Warning: items "{itm}" have repeat id in db, may cause data not write in.')


if __name__ == '__main__':
    sa = StockAnalyze()
    sa.update_data()
    # a= sa.db.query_db('basic', 'stock_basic')
