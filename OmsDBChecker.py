"""
读取和检查 Oms.OrderBook
    1) 最新时间，是否更新
    2) 是否有长时间未成交的订单
    3) 是否出现大量拒绝单

数据:
    a) 最新获取的全部Order数据：
    b) 上一次获取的全部Order数据；
    c) a) - b)
"""


import os
import sys
import shutil
from typing import List, Dict
import json
from queue import Queue
import threading
from datetime import datetime, date, timedelta
from time import sleep
import argparse
import logging

sys.path.append(os.path.dirname(__file__))

from pyptools.pyptools_oms import OmsDbManagement, Order, Direction, OrderState
from helper.simpleLogger import MyLogger
from helper.scheduler import ScheduleRunner
from helper.tp_WarningBoard import run_warning_board
from helper.csvreader import HeaderCsvReader

# 参数设置
# arg_parser = argparse.ArgumentParser()
# arg_parser.add_argument('-d', '--days', help='数据间隔大于N天则视为错误', default=15)
# args = arg_parser.parse_args()
# start_date = args.start

#


class OmsChecker(ScheduleRunner):
    def __init__(
            self, oms_db: OmsDbManagement,
            running_time: list, loop_interval=60,
            warning_gap: int = 60,
            unfilled_order_warning_gap: int = 180,     # 未成交单的报错时间
            max_warning_board_count=10,
            _logger=logging.Logger('OmsChecker'),
            **kwargs
    ):
        super(OmsChecker, self).__init__(running_time=running_time, loop_interval=loop_interval, logger=_logger)

        self._db: OmsDbManagement = oms_db
        self._warning_gap = float(warning_gap)
        self._unfilled_order_warning_gap = float(unfilled_order_warning_gap)
        self._unfilled_order_list = []
        self._max_warning_board_count = max_warning_board_count

        # 检查 线程
        self._thread = None
        # 警告 线程
        self._thread_warning: List[threading.Thread] = []
        # 每次结束warning后，都将 结束warning的时间点，作为对比时间
        self.last_warn = datetime.now() - timedelta(minutes=1)

    def _running_loop(self):
        self.last_warn = datetime.now() - timedelta(minutes=1)
        while self._schedule_in_running:
            self._checking_order_books()

            # 检测间隔
            if not self._schedule_in_running:
                break
            sleep(self._schedule_loop_interval)

    # 检查OrderBook
    def _checking_order_books(self):
        # 读取 OrderBook 数据
        l_order_books: List[Order] = self._db.query_orders()
        # 读取旧数据
        p_data = os.path.join(path_output_root, 'OrderBooks.csv')
        # p_data_bak = os.path.join(path_output_root, 'OrderBooks.csv.%s.bak' % datetime.now().strftime('%Y%m%d%H%M%S'))
        # old_data = {}
        # if os.path.isfile(p_data):
        #     old_data: Dict[str, dict] = HeaderCsvReader(
        #         key='InternalId',
        #         values='Account,Trader,Ticker,Direction,LimitPrice,Volume,OrderStatus,'
        #                'OrderType,TradedPrice,TradedVolume,CreateTime,UpdateTime,CacheTime,FillingTime'.split(',')
        #     ).read(p_data)
        # 备份
        with open(p_data, 'w', encoding='utf-8') as f:
            f.write('InternalId,ExternalId,Account,Trader,Ticker,Direction,LimitPrice,Volume,OrderStatus,OrderType,'
                    'TradedPrice,TradedVolume,HedgeFlag,OffsetFlag,Remark,BatchId,IsBatchOrder,'
                    'CreateTime,UpdateTime,CacheTime,FillingTime\n')
            for order_book in l_order_books:
                f.write(str(order_book) + '\n')
        # shutil.copyfile(src=p_data, dst=p_data_bak)

        # 检查
        # 1) 最新order
        l_order_books.sort(key=lambda x: x.CreateTime)
        if len(l_order_books) == 0:
            self.logger.info('OrderBook is empty')
            return
        latest_order = l_order_books[-1]
        self.logger.info(f'最新Order, '
                         f'{latest_order.CreateTime.strftime("%Y-%m-%d %H:%M:%S")}, '
                         f'{latest_order.Trader}, '
                         f'{latest_order.Ticker}, '
                         f'{str(Direction(latest_order.Direction).name)}, '
                         f'{latest_order.Volume}, {latest_order.LimitPrice}')
        # 2) 是否有未成交单
        # 新数据
        # l_order_books_new: List[Order] = []
        # if old_data:
        #     for order_data in l_order_books:
        #         if order_data.InternalId not in old_data.keys():
        #             l_order_books_new.append(order_data)
        # else:
        #     l_order_books_new = l_order_books.copy()
        # 查找未成交订单
        l_unfilled_order = []
        for order in l_order_books:
            if order.OrderStatus not in [4, 5]:
                if (datetime.now() - order.CreateTime).seconds >= self._unfilled_order_warning_gap:
                    l_unfilled_order.append(order)
        l_unfilled_order_new = [_ for _ in l_unfilled_order if _ not in self._unfilled_order_list]
        if l_unfilled_order_new:
            for order in l_unfilled_order_new:
                self.logger.warning(
                    f'此订单长时间未成交或撤单, '
                    f'{order.Trader}, {order.Ticker}, '
                    f'{str(Direction(order.Direction).name)}, '
                    f'{str(order.Volume)}, {str(order.LimitPrice)}, '
                    f'{order.CreateTime.strftime("%Y-%m-%d %H:%M:%S")}'
                )
            # 弹框报警
            self.warning('订单长时间未成交或撤单')
            self._unfilled_order_list = l_unfilled_order

    # 调用 warning board 弹框报错。会阻塞
    def warning(self, warning_string='交易间隔超时'):
        def _run():
            run_warning_board(warning_string, timeout_continue=6000)

        self._check_warning_thread()
        if len(self._thread_warning) >= self._max_warning_board_count:
            self.logger.warning('弹框报错数量过多, 暂不弹框')
            return
        else:
            t = threading.Thread(target=_run)
            self._thread_warning.append(t)
            t.start()
            sleep(0.1)

    def _check_warning_thread(self):
        for t in self._thread_warning:
            if not t.is_alive():
                self._thread_warning.remove(t)

    def _start(self):
        """
        """
        self._thread = threading.Thread(target=self._running_loop)
        self._thread.start()

    def _end(self):
        self._schedule_in_running = False
        self._thread.join()
        self.logger.info('\t线程已终止')


def _change_datetime(running_time):
    return [
        [datetime.strptime(i[0], '%H%M%S').time(), datetime.strptime(i[1], '%H%M%S').time()]
        for i in running_time
    ]


if __name__ == '__main__':
    s_dt = datetime.now()
    today_dt = datetime.now().date()
    path_root = os.path.abspath(os.path.dirname(__file__))
    path_config = os.path.join(path_root, 'Config', 'Config.json')
    path_output_root = os.path.join(path_root, 'Output_OrderBooks')
    logger = MyLogger('OmsDBChecker', output_root=os.path.join(path_root, 'logs'))
    logger.info('Stated...')
    if os.path.isdir(path_output_root):
        shutil.rmtree(path_output_root)
        sleep(0.5)
    os.makedirs(path_output_root)

    # 读取db信息
    d_config = json.loads(open(path_config).read(), encoding='utf-8')
    d_db_config = d_config['db']
    # 读取新数据
    oms_object = OmsDbManagement(**d_db_config)
    checker = OmsChecker(
        oms_db=oms_object, loop_interval=d_config['loop_interval'],
        running_time=_change_datetime(d_config['running_time']),
        unfilled_order_warning_gap=d_config['unfilled_order_warning_gap'],
        _logger=logger,
    )
    checker.start_loop()

    oms_object.close()
