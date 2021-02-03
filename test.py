from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime  # For datetime objects
import os.path  # To manage paths
import sys  # To find out the script name (in argv[0])

import backtrader as bt
import backtrader.feeds as btfeeds
import backtrader.indicator as btind
import pandas as pd
import numpy as np
import logging
import pymysql


def print_obj(obj):
    print(obj.__dict__)

class DB_Manage():
    def get_minute_record():
        conn = pymysql.connect(host="159.138.110.208", user="root", passwd="MSbolang667496", db="huice", charset="utf8")
        sql_query = "SELECT * FROM minute_record WHERE datatime  BETWEEN '2020-11-25 00:00:00' AND '2020-12-01 00:00:59' ORDER BY time_stamp ASC"
        dataframe = pd.read_sql(sql_query, con=conn)
        # print(dataframe)
        conn.close()  # 使用完后记得关掉
        return dataframe
    #######################################reset db##################################
    def init_table_trend_index():
        conn = pymysql.connect(host="159.138.110.208", user="root", passwd="MSbolang667496", db="huice", charset="utf8")
        sql_query = "UPDATE trend_index set indicator=5,price=0,then_time='1995-01-01 00:00:00',next_update_time_stamp=0 where 1"
        cursor = conn.cursor()
        cursor.execute(sql_query)
        conn.commit() #执行update操作时需要写这个，否则就会更新不成功
        conn.close()

    def init_table_record():
        conn = pymysql.connect(host="159.138.110.208", user="root", passwd="MSbolang667496", db="huice", charset="utf8")
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE record;")
        conn.commit()
        conn.close()

    def init_table_warrior():
        print("====init_table_warrior=====")
        conn = pymysql.connect(host="159.138.110.208", user="root", passwd="MSbolang667496", db="huice", charset="utf8")
        cursor = conn.cursor()
        cursor.execute("UPDATE warrior SET profit=0, liquidation_value=0, liquidation_value=0, status=0, buy_quantity=0 WHERE 1;")
        conn.commit()
        conn.close()

    #######################################reset db end##############################
    def updataTrendIndex(data):
        conn = pymysql.connect(host="159.138.110.208", user="root", passwd="MSbolang667496", db="huice", charset="utf8")
        cursor = conn.cursor()

        sql_query = "UPDATE trend_index SET indicator=0 WHERE indicator<0"
        # pd.read_sql(sql_query, con=conn)
        cursor.execute(sql_query)

        sql_query2 = "UPDATE trend_index SET indicator=10 WHERE indicator>10"
        # pd.read_sql(sql_query2, con=conn)
        cursor.execute(sql_query2)

        time_stamp =  data.time_stamp.strftime("%Y-%m-%d %H:%M:%S")
        sql_query3 = "UPDATE trend_index SET indicator = CASE "
        sql_query3 +=  " when " + data.open + " >price THEN indicator+1 "
        sql_query3 += " when " + data.open + " <price THEN indicator-1 "
        sql_query3 += " END, price="+data.open+", then_time="+time_stamp+", next_update_time_stamp=id*60+"+time_stamp+" WHERE next_update_time_stamp<="+time_stamp+" AND indicator BETWEEN 0 AND 10; "
        # pd.read_sql(sql_query3, con=conn)
        cursor.execute(sql_query3)
        conn.commit()
        conn.close()

class DT_Line(bt.Indicator):
    #在附加图层里面定义两条线 U(up)  D(down)
    lines = ('U','D')
    #参数定义默认2期 （period=2 代表默认两日内的最高点和最低点） （k_u 和 k_d 默认值0.7 代表画出的线取值0.7？？）
    params = (('period',120),('k_u',0.7),('k_d',0.7))

    def __init__(self):
        #第3日开始购买
        self.addminperiod(self.params.period+1)

    def next(self):
        #HH 两日最高价（如果period=2的时候）
        HH = max(self.data.high.get(-1,size=self.params.period))
        # print(self.data.high.get(1))
        # print(HH)
        #两日最低收盘价
        LC = min(self.data.close.get(-1,size=self.params.period))

        #两日最高收盘价
        HC = max(self.data.close.get(-1,size=self.params.period))

        #两日最低价
        LL = min(self.data.low.get(-1,size=self.params.period))

        R = max(HH-LC,HC-LL)

        self.lines.U[0] = self.data.open[0]+self.params.k_u*R
        self.lines.D[0] = self.data.open[0]-self.params.k_d*R


#data0是谁   data1又是谁
class DualThrust(bt.Strategy):

    #(优化器)
    # params = (('period',2),('k_u',0.7),('k_d',0.7))
    def __init__(self):
        self.dataclose = self.data0.close

        #（优化器） 使用优化器之后参数需要传递进line
        #self.D_Line = DT_Line(self.data1, period=self.p.period,k_u=self.p.k_u,k_d=self.p.k_d)

        self.D_Line = DT_Line(self.data1)

        self.D_Line = self.D_Line() #调用日线的自身 它就会把自己映射到分钟线维度的空白处
        self.D_Line.plotinfo.plotmaster = self.data0  #把日线时间维度画到分钟线里面

        #上穿买入信号
        self.buy_signal = bt.indicators.CrossOver(self.dataclose, self.D_Line.U)

        #下穿卖出信号
        self.sell_signal = bt.indicators.CrossDown(self.dataclose, self.D_Line.D)

    #开始执行 调用的方法 start 执行调用1次
    def start(self):
        #打印一次
        print("the world call me!")

    # 每执行一个bar 就会执行这个函数
    def prenext(self):
        print("=====prenext=====")
        # print(self.data[0])
        # print(self.data.datetime[0])
        # print(pd.to_datetime(self.data.datetime[0]))
        #
        # print(self.data.open[0])
        # print(self.data.close[0])
        # print(self.data.low[0])
        # print(self.data.high[0])
        # print(self.data.volume[0])
        # print_obj(self.data)

        # print(self.data.datatime[0])
        print("=====not mature====")
        # DB_Manage.updataTrendIndex(brf_minutes_bar)

    def next(self):
        #如果没有仓位 并且上穿了
        # if self.getposition().opened<=0 and self.buy_signal[0] == 1:
        #     #买入
        #
        #     self.order = self.buy()
        #
        # #如果有仓位并且下穿了
        # if self.getposition().opened>=0 and self.sell_signal[0] == 1:
        #     #卖出
        #     self.order = self.sell()

        #如果没有仓位 并且是买入信号  平仓 买入   什么逻辑啊这是
        if self.getposition().size <= 0 and self.buy_signal[0] == 1:
            # self.order = self.close()
            self.order = self.buy()
            print('buy:', self.data0.open.get(-1))

        # #如果有仓位并且是卖出信号  平仓 卖出
        if self.getposition().size > 0 and self.sell_signal[0] == 1:
            # self.order = self.close()
            self.order = self.sell()
            print('sell:', self.data0.open.get(-1))
    #（优化器）使用优化器 进行多种参数批量回测时 可以用stop方法打印出收益等信息
    # def stop(self):
    #     print('period: %s, k_u: %s, k_d: %s, final_velue: %.2f' %
    #           (self.p.period,self.p.k_u, self.p.k_d, self.broker.getvalue())
    #           )

if __name__=='__main__':
    #1. create a cerebro
    cerebro = bt.Cerebro()
    #关闭默认的观察者
    # cerebro = bt.Cerebro(stdstats=False)

    #=================添加观察者
    # cerebro.addobserver(bt.observers.Broker) 默认就有
    cerebro.addobserver(bt.observers.Trades)
    # cerebro.addobserver(bt.observers.BuySell) 默认就有

    cerebro.addobserver(bt.observers.DrawDown)
    cerebro.addobserver(bt.observers.Value)
    cerebro.addobserver(bt.observers.TimeReturn)

    #================添加分析者
    cerebro.addanalyzer(bt.analyzers.SharpeRatio)
    cerebro.addanalyzer(bt.analyzers.DrawDown)
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer)
    cerebro.addanalyzer(bt.analyzers.Transactions)

    #================添加记录者
    # cerebro.addwriter(bt.WriterFile,csv=True,out='min-lin.csv')

    #2. add data feed
    #2.1 create adata feed
    # dataframe = pd.read_csv('./2021-01-21-kline.log')

    #改1 读取mysql
    # dataframe = pd.read_csv('./minute_record.csv')

    #######################开始执行####################
    # 重置DB
    DB_Manage.init_table_record()
    DB_Manage.init_table_warrior()
    DB_Manage.init_table_trend_index()
    # 重置DB END

    #获取分钟数据
    dataframe = DB_Manage.get_minute_record()
    dataframe['datatime'] = pd.to_datetime(dataframe['datatime'])
    dataframe.set_index('datatime', inplace=True)

    brf_minutes_bar = bt.feeds.PandasData(
                           dataname=dataframe,
                           fromdate=datetime.datetime(2020, 11, 25, 0,1),
                           todate=datetime.datetime(2020, 11, 30, 23,59),
                           timeframe=bt.TimeFrame.Minutes)

    cerebro.adddata(brf_minutes_bar)
    cerebro.resampledata(brf_minutes_bar, timeframe=bt.TimeFrame.Minutes)

    #分钟线的数据
    # brf_min_bar = bt.feeds.PandasData(
    #                        dataname=dataframe,
    #                        fromdate=datetime.datetime(2016, 10, 1),
    #                        todate=datetime.datetime(2017, 1, 28),
    #                        timeframe=bt.TimeFrame.Minutes)

    #timeframe=bt.TimeFrame.Minutes 告诉backtrader 这是分钟级别的数据  问题：有没有秒级别的数据呢？ 答：有  （重点）

    #2.2Add the Data Feed to Cerebro
    # cerebro.adddata(brf_min_bar)

    #把日线维度加入到大脑 resampledata重要  把日线维度映射到分钟 把分钟K线变成日K线 加入到大脑里面 （重点）
    # cerebro.resampledata(brf_min_bar, timeframe=bt.TimeFrame.Days)

    #3 add strategy
    cerebro.addstrategy(DualThrust)
    #(优化器) 使用优化器之后
    #cerebro.optstrategy(DualThrust,period=range(1,5),k_u=[n/10.0 for n in range(2,10)],k_u=[n/10.0 for n in range(2,10)])

    #设置本金
    cerebro.broker.set_cash(1000)

    #设置手续费
    cerebro.broker.setcommission(commission=0.2, name='test001')

    #设置滑点 固定滑点
    cerebro.broker.set_slippage_fixed(fixed=5)

    #设置按百分比的滑点
    #cerebro.broker.set_slippage_perc(perc=0.05)


    #4 Run
    # cerebro.run()
    #只有开启了addanalyzer run的时候就可以返回数据  【0】代表第一次策略
    res = cerebro.run()[0]

    print('SharpeRatio:',res.analyzers.sharperatio.get_analysis())
    print('DrawDown:',res.analyzers.drawdown.get_analysis())
    print('TradeAnalyzer:', res.analyzers.tradeanalyzer.get_analysis())
    print('Transactions:', res.analyzers.transactions.get_analysis())

    print('==============SharPeratio==================')
    sharperatio_data = res.analyzers.sharperatio.get_analysis()
    print('夏普比率:%s' % sharperatio_data['sharperatio'])

    print('==============Drow Down==================')
    drawdown_data = res.analyzers.drawdown.get_analysis()
    print('MAX Drow Down:%s' % drawdown_data['max']['drawdown'])
    print('MAX Money Down:%s' % drawdown_data['max']['moneydown'])

    print('==============TradeAnalyzer==================')
    tradeanalyzer_data = res.analyzers.tradeanalyzer.get_analysis()
    print('===won===')
    print(type(tradeanalyzer_data))
    won = tradeanalyzer_data.get("won")
    print(type(won))
    if won is not None:
        print('won ratio:%s ' % (tradeanalyzer_data['won']['total']/float(tradeanalyzer_data['won']['total']+tradeanalyzer_data['lost']['total'])))
        print('won_hits: %s ' % tradeanalyzer_data['won']['total'])
        print('won_pnl-->total: %s, average: %s, max: %s' %
          (tradeanalyzer_data['won']['pnl']['total'],
           tradeanalyzer_data['won']['pnl']['average'],
           tradeanalyzer_data['won']['pnl']['max'])
          )

        print('===lost===')
        print('lost_hits:%s' % tradeanalyzer_data['lost']['total'])
        print('lost_pnl-->total: %s, average: %s, max: %s' %
              (tradeanalyzer_data['lost']['pnl']['total'],
               tradeanalyzer_data['lost']['pnl']['average'],
               tradeanalyzer_data['lost']['pnl']['max'])
              )

        print('===long position===')
        print('long_hits:%s' % tradeanalyzer_data['long']['total'])
        print('long_pnl-->total: %s, average: %s' %
              (tradeanalyzer_data['long']['pnl']['total'],
               tradeanalyzer_data['long']['pnl']['average'])
              )

        print('===short position===')
        print('short_hits:%s' % tradeanalyzer_data['short']['total'])
        print('short_pnl-->total: %s, average: %s' %
              (tradeanalyzer_data['short']['pnl']['total'],
              tradeanalyzer_data['short']['pnl']['average'])
              )
        # print('==============Transactions==================')
    # transactions_data = res.analyzers.transactions.get_analysis()

    #画图
    #5 Plot result
    cerebro.plot()
