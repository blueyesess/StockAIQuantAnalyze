# -*- coding:utf-8 -*-
import tushare as ts
import time
from StockTool import *

# 数据源类，从api获取数据
class DataSource():
    def __init__(self, day_limit=[], min_limit=[]):
        # token
        tushare_token = '424139bf631af25972d3101d8e2b125a269b89bf575811ac8aaac819'
        # initialize
        self.id_dic = self.initial()
        self.tushare_api = ts.pro_api(tushare_token)
        self.stock_pool = self.query_api(access='basic', id='stock_basic')['ts_code'].to_dict().values()
        self.day_limit = day_limit
        self.min_limit = min_limit

    # 初始化参数, 返回初始化id字典
    def initial(self):
        # 储存tushare允许的访问字
        # id_dic['访问域']['类型']=[允许的id]
        id_dic = {
            'basic': {},
            'market': {},
            'finance': {},
            'ref': {},
            'index': {}
        }
        # 子类别允许id
        # 基础类
        id_dic['basic']['stock_basic'] = ['ts_code']
        id_dic['basic']['trade_cal'] = ['start_date', 'end_date']
        id_dic['basic']['namechange'] = ['ts_code']
        # id_dic['basic']['hs_const'] = ['hs_type']
        id_dic['basic']['stock_company'] = ['ts_code']
        # id_dic['basic']['stk_managers'] = ['ts_code']
        # id_dic['basic']['stk_rewards'] = ['ts_code']
        id_dic['basic']['new_share'] = ['start_date', 'end_date']
        id_dic['basic']['bak_basic'] = ['ts_code']
        # 行情类
        id_dic['market']['daily'] = ['ts_code', 'start_date', 'end_date']
        id_dic['market']['weekly'] = ['ts_code', 'start_date', 'end_date']
        id_dic['market']['monthly'] = ['ts_code', 'start_date', 'end_date']
        id_dic['market']['pro_bar'] = ['ts_code', 'freq', 'adj', 'start_date', 'end_date']
        id_dic['market']['adj_factor'] = ['ts_code', 'start_date', 'end_date']
        id_dic['market']['suspend'] = ['ts_code']
        id_dic['market']['suspend_d'] = ['ts_code', 'start_date', 'end_date']
        id_dic['market']['daily_basic'] = ['ts_code', 'start_date', 'end_date']
        # id_dic['market']['pro_bar'] = ['ts_code', 'asset', 'freq']
        id_dic['market']['moneyflow'] = ['ts_code', 'start_date', 'end_date']
        id_dic['market']['stk_limit'] = ['ts_code', 'asset', 'freq']
        id_dic['market']['day_limitt'] = ['ts_code', 'asset', 'freq']
        id_dic['market']['moneyflow_hsgt'] = ['asset', 'freq']
        id_dic['market']['hsgt_top10'] = ['start_date', 'end_date']
        id_dic['market']['hk_hold'] = ['ts_code', 'start_date', 'end_date']
        id_dic['market']['ggt_daily'] = ['start_date', 'end_date']
        id_dic['market']['ggt_monthly'] = ['start_date', 'end_date']
        id_dic['market']['bak_daily'] = ['ts_code', 'start_date', 'end_date']
        # 财务类
        id_dic['finance']['income'] = ['ts_code', 'start_date', 'end_date']
        id_dic['finance']['balancesheet'] = ['ts_code', 'start_date', 'end_date']
        id_dic['finance']['cashflow'] = ['ts_code', 'start_date', 'end_date']
        id_dic['finance']['forecast'] = ['ts_code', 'start_date', 'end_date']
        id_dic['finance']['express'] = ['ts_code', 'start_date', 'end_date']
        id_dic['finance']['dividend'] = ['ts_code']
        id_dic['finance']['fina_indicator'] = ['ts_code', 'start_date', 'end_date']
        id_dic['finance']['fina_audit'] = ['ts_code', 'start_date', 'end_date']
        id_dic['finance']['fina_mainbz'] = ['ts_code', 'start_date', 'end_date']
        id_dic['finance']['disclosure_date'] = ['ts_code', 'start_date', 'end_date']
        # 市场参考类
        id_dic['ref']['ggt_top10'] = ['ts_code', 'start_date', 'end_date']
        id_dic['ref']['margin'] = ['start_date', 'end_date']
        id_dic['ref']['margin_detail'] = ['ts_code', 'start_date', 'end_date']
        id_dic['ref']['top10_holders'] = ['ts_code', 'start_date', 'end_date']
        id_dic['ref']['top10_floatholders'] = ['ts_code', 'start_date', 'end_date']
        id_dic['ref']['top_list'] = ['trade_date']
        id_dic['ref']['top_inst'] = ['trade_date']
        id_dic['ref']['pledge_stat'] = ['ts_code']
        id_dic['ref']['pledge_detail'] = ['ts_code']
        id_dic['ref']['repurchase'] = ['start_date', 'end_date']
        id_dic['ref']['concept'] = ['src']
        id_dic['ref']['concept_detail'] = ['ts_code']
        id_dic['ref']['share_float'] = ['ts_code', 'start_date', 'end_date']
        id_dic['ref']['block_trade'] = ['ts_code', 'start_date', 'end_date']
        id_dic['ref']['stk_account'] = ['start_date', 'end_date']
        id_dic['ref']['stk_account_old'] = ['start_date', 'end_date']
        id_dic['ref']['stk_holdernumber'] = ['ts_code', 'start_date', 'end_date']
        id_dic['ref']['stk_holdertrade'] = ['ts_code', 'start_date', 'end_date']
        id_dic['ref']['broker_recommend'] = ['month']
        # 指数类
        id_dic['index']['index_basic'] = ['ts_code']
        id_dic['index']['index_daily'] = ['ts_code', 'start_date', 'end_date']
        id_dic['index']['index_weekly'] = ['ts_code', 'start_date', 'end_date']
        id_dic['index']['index_monthly'] = ['ts_code', 'start_date', 'end_date']
        id_dic['index']['index_weight'] = ['ts_code', 'start_date', 'end_date']
        id_dic['index']['index_dailybasic'] = ['ts_code', 'start_date', 'end_date']
        id_dic['index']['index_classify'] = ['index_code']
        id_dic['index']['index_member'] = ['index_code', 'ts_code']
        id_dic['index']['daily_info'] = ['ts_code', 'start_date', 'end_date']
        id_dic['index']['sz_daily_info'] = ['ts_code', 'start_date', 'end_date']
        id_dic['index']['ths_index'] = ['ts_code']
        id_dic['index']['ths_daily'] = ['ts_code', 'start_date', 'end_date']
        id_dic['index']['ths_member'] = ['ts_code']
        id_dic['index']['index_global'] = ['ts_code', 'start_date', 'end_date']
        return id_dic

    # API查询函数， 返回查询结果
    # access [string]   : 用于指定访问基本类
    # id     [sting]    : 用于指定访问的数据域
    def query_api(self, fields=[], **kwargs):
        # print(fields, kwargs)
        # 检查是否指定access和id
        if not kwargs.get('access', None) or not kwargs.get('id', None):
            print(f'Error: need to specify "access" and "id".')
            exit(1)
        else:
            filed_access = kwargs['access']
            filed_id = kwargs['id']

        # 检查其余参数是否合规
        if not self.id_dic.get(filed_access, None):
            print(f'Error: no support filed "{filed_access}", support is {list(self.id_dic.keys())}')
            exit(1)

        if not self.id_dic[filed_access].get(filed_id, None):
            print(f'Error: no support filed "{filed_id}", support is {list(self.id_dic[filed_access].keys())}')
            exit(1)

        query_opt = ''
        for k, w in kwargs.items():
            if k is 'access' or k is 'id':
                continue
            elif not k in self.id_dic[filed_access][filed_id]:
                print(f'Error: no support filed "{k}", support is {self.id_dic[filed_access][filed_id]}')
                exit(1)
            else:
                query_opt += f'{k}="{w}", '

        # 向API发出查询指令
        # print(kwargs.keys())
        if fields:
            query_cmd = f'self.tushare_api.query("{filed_id}", {query_opt[:-2]}, fields={fields})'
        else:
            query_cmd = f'self.tushare_api.query("{filed_id}", {query_opt[:-2]})'
        # print(query_cmd)
        try:
            # print(query_cmd)
            return eval(query_cmd)
        except Exception:
            print(f'Error: command query command is "{query_cmd}"')
            # 如果异常是day访问次数限制，返回None
            if filed_id in self.day_limit:
                return None
            # 如果异常min访问次数限制，等待1分钟后继续访问
            elif filed_id in self.min_limit:
                for i in range(60):
                    time.sleep(1)
                    bar(now=i, total=60, prefix='Wait:')
                return eval(query_cmd)
            else:
                return eval(query_cmd)

if __name__ == '__main__':
    ds = DataSource()
    # a = ds.query_api(access='basic', id='stock_basic')
    #   print(a)


