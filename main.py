"""
读取和检查 Oms.OrderBook
    1) 检查并显示最新order，不报错，方便平时查看，确认数据已经传到db
    2) 检查是否有未成交单，报错。同一订单不会重复报错。
    3) 检查是否有（新增的）未成交订单，报错

"""


import os
import sys
import shutil
import json
from datetime import datetime, date, timedelta
from time import sleep
import argparse

PATH_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.append(PATH_ROOT)

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('-o', '--output', default=os.path.join(PATH_ROOT, 'Output_OrderBooks'))
args = arg_parser.parse_args()
PATH_OUTPUT_ROOT = os.path.abspath(args.output)

from OmsDBChecker import (
    OmsCheckerEngine,
    OmsOrderBookLatestOrderChecker, OmsOrderBookRepeatedOrderChecker, OmsOrderBookUnfilledOrderChecker,
    OmsTradeBookDataDownloadOnlyChecker, OmsPositionBookDataDownloadOnlyChecker
)
from pyptools.pyptools_oms import OmsDbManagement, Order, Direction, OrderState
from helper.simpleLogger import MyLogger


def _change_datetime(running_time):
    return [
        [datetime.strptime(i[0], '%H%M%S').time(), datetime.strptime(i[1], '%H%M%S').time()]
        for i in running_time
    ]


if __name__ == '__main__':
    logger = MyLogger('OmsDBChecker', output_root=os.path.join(PATH_ROOT, 'logs'))
    logger.info('Stated...')
    if os.path.isdir(PATH_OUTPUT_ROOT):
        shutil.rmtree(PATH_OUTPUT_ROOT)
        sleep(0.1)
    os.makedirs(PATH_OUTPUT_ROOT)

    # 读取db信息
    path_config = os.path.join(PATH_ROOT, 'Config', 'Config.json')
    d_config = json.loads(open(path_config, encoding='utf-8').read(), encoding='utf-8')
    d_db_config = d_config['db']
    # 读取MSWarning信息
    path_mswarning_config = os.path.join(PATH_ROOT, 'Config', 'MSWarningConfig.json')
    d_ms_warning_config = json.loads(open(path_mswarning_config, encoding='utf-8').read(), encoding='utf-8')

    #
    oms_object = OmsDbManagement(**d_db_config)
    engine = OmsCheckerEngine(
        oms_db=oms_object, loop_interval=d_config['loop_interval'],
        output_root=PATH_OUTPUT_ROOT,
        ms_warning_config=d_ms_warning_config,
        running_time=_change_datetime(d_config['running_time']),
        _logger=logger,
    )
    # 获取打印 最新order
    engine.add_checker(OmsOrderBookLatestOrderChecker(engine))
    # 检查 高配重复下单
    engine.add_checker(OmsOrderBookRepeatedOrderChecker(
        engine, checking_time=d_config['repeated_order_checking_window'],
        max_order=d_config['max_repeated_order'],
        output_root=PATH_OUTPUT_ROOT
    ))
    # 检查 长时间未成交
    engine.add_checker(OmsOrderBookUnfilledOrderChecker(
        engine, unfilled_order_warning_gap=d_config['unfilled_order_warning_gap'],
        output_root=PATH_OUTPUT_ROOT
    ))
    # # 不检查，下载 trade
    # engine.add_checker(OmsTradeBookDataDownloadOnlyChecker())
    # # 不检查，下载 position
    # engine.add_checker(OmsPositionBookDataDownloadOnlyChecker())

    engine.start_loop()

    oms_object.close()
    logger.info('Closed process')
