"""
# TODO db.query_order() id是否会发生变化。是不会变得，但是不知道为什么

"""


import os
import sys
from typing import List, Dict
import threading
from datetime import datetime, date, timedelta
from time import sleep
import logging
from abc import ABCMeta
from abc import abstractmethod
from collections import namedtuple, defaultdict

sys.path.append(os.path.dirname(__file__))

from pyptools.pyptools_oms import OmsDbManagement, Order, Trade, TraderPosition, Direction, OrderState
from helper.simpleLogger import MyLogger
from helper.scheduler import ScheduleRunner
from helper.tp_WarningBoard import run_warning_board
from helper.PyMessageClient import MessageClient


#
OmsDbItems = namedtuple('OmsDbItems', ['Order', 'Trade', 'Position', 'OrderLogs', 'TradeLogs', 'PositionLogs'])


class OmsCheckerEngine(ScheduleRunner):
    """
    定期获取数据，并调用 检查程序
    """
    def __init__(
            self,
            oms_db: OmsDbManagement,
            running_time: list,
            output_root: str,
            ms_warning_config: dict,
            max_warning_board_count=10,
            loop_interval=60,
            _logger=logging.Logger('OmsChecker'),
            **kwargs
    ):
        super(OmsCheckerEngine, self).__init__(running_time=running_time, loop_interval=loop_interval, logger=_logger)

        self._db: OmsDbManagement = oms_db
        self._max_warning_board_count = max_warning_board_count
        self.output_root = os.path.abspath(output_root)
        if not os.path.isdir(self.output_root):
            os.makedirs(self.output_root)
        self._ms_warning_method = self._ms_warning(**ms_warning_config)
        # 检查 线程，唯一
        self._thread = None
        # 初始化数据
        self._checker: List[OmsCheckerBase] = []        # 检查器
        self._required_items: List[OmsDbItems] = []     # 需要订阅的oms db表
        self._db_orders: List[Order] = []       # 存放从db获取的 order数据
        self._db_trades: List[Trade] = []       # 存放从db获取的 trade数据
        self._db_positions: List[TraderPosition] = []       # 存放从db获取的 position数据
        self._list_warning_string = []      # 存放需要弹框报错的信息
        self._thread_warning = []

    @property
    def db(self) -> OmsDbManagement:
        return self._db
        
    def add_checker(self, checker):
        self._checker.append(checker)
        if checker.item_name not in self._required_items:
            self._required_items.append(checker.item_name)

    # 数据获取，输出csv
    def _query_oms_data(self, is_output_file=True):
        # 刷新 session 缓存，否则每次query都会先提取 缓存中已经有的数据，而db的对应行的值是可能会发生变化的，所以必须先刷新
        self.db.session.commit()
        #
        if OmsDbItems.Order in self._required_items:
            output_file = os.path.join(self.output_root, 'Order.csv')
            self._db_orders: List[Order] = self.db.query_orders()
            if is_output_file:
                self._db.data_to_csv(output_file, self._db_orders)
        if OmsDbItems.Trade in self._required_items:
            output_file = os.path.join(self.output_root, 'Trade.csv')
            self._db_trades: List[Trade] = self.db.query_trades()
            if is_output_file:
                self._db.data_to_csv(output_file, self._db_trades)
        if OmsDbItems.Position in self._required_items:
            output_file = os.path.join(self.output_root, 'Position.csv')
            self._db_positions: List[TraderPosition] = self.db.query_positions()
            if is_output_file:
                self._db.data_to_csv(output_file, self._db_positions)
    
    def _get_oms_item_data(self, query_item: OmsDbItems) -> list:
        if query_item == OmsDbItems.Order:
            return self._db_orders
        if query_item == OmsDbItems.Trade:
            return self._db_trades
        if query_item == OmsDbItems.Position:
            return self._db_positions
        self.logger.error('输入的OmsDbItems错误')
        raise ValueError

    def _running_loop(self):
        while self._schedule_in_running:
            # 从数据库获取数据,并输出文件
            self._query_oms_data(is_output_file=True)
            # 逐个调用checker,输出数据并检查
            for _checker in self._checker:
                _checker.check(self._get_oms_item_data(_checker.item_name))
            # 报错
            self._warning()
            # 检测间隔
            if not self._schedule_in_running:
                break
            sleep(self._schedule_loop_interval)

    def _start(self):
        """
        """
        for _checker in self._checker:
            _checker.renew()
        self._thread = threading.Thread(target=self._running_loop)
        self._thread.start()

    def _end(self):
        self._schedule_in_running = False
        self._thread.join()
        self.logger.info('\t线程已终止')

    def _ms_warning(self, ip, port, key, warning_value):
        mc = MessageClient(ip=ip, port=port)
        key = key
        warning_value = warning_value

        def _warning(error: bool):
            if error:
                mc.sendmessage(key=key,  message=warning_value + ' ' + datetime.now().strftime('%Y/%m/%d %H:%M:%S'))
            else:
                mc.sendmessage(key=key,  message='Checked' + ' ' + datetime.now().strftime('%Y/%m/%d %H:%M:%S'))
        return _warning

    # 调用 warning board 弹框报错。
    def _warning(self):
        def _check_warning_thread():
            # 避免重复弹框
            for t in self._thread_warning:
                if not t.is_alive():
                    self._thread_warning.remove(t)

        def _run(warning_string):
            run_warning_board(warning_string, timeout_continue=6000)


        # 检查弹框报错
        _check_warning_thread()
        # 没有新报错
        if not self._list_warning_string:
            if self._thread_warning:
                self._ms_warning_method(error=True)
            else:
                self._ms_warning_method(error=False)
            return
        # MSWarning 报错
        self._ms_warning_method(error=True)
        # 检查弹框数超过限制
        if len(self._thread_warning) >= self._max_warning_board_count:
            for n in range(len(self._list_warning_string)):
                s = self._list_warning_string.pop()
                self.logger.warning(f'弹框报错数量过多, 暂不弹框. 报错内容: {str(s)}')
            return
        else:
            for n in range(len(self._list_warning_string)):
                s = self._list_warning_string.pop()
                t = threading.Thread(target=_run, args=(s,))
                self._thread_warning.append(t)
                t.start()
                sleep(0.1)

    def send_warning(self, s: str):
        self._list_warning_string.append(str(s))


"""
- 检查最新order，并打印，（间隔报错）
- 检查
"""



class OmsCheckerBase(metaclass=ABCMeta):
    item_name: OmsDbItems
    _engine: OmsCheckerEngine

    @abstractmethod
    def check(self, data):
        pass

    @abstractmethod
    def renew(self):
        pass

    @property
    def engine(self) -> OmsCheckerEngine:
        return self._engine

    @property
    def db(self) -> OmsDbManagement:
        return self._engine.db


class OmsOrderBookLatestOrderChecker(OmsCheckerBase):
    """ 最新order  不报错 """
    def __init__(self, engine: OmsCheckerEngine):
        super().__init__()
        self.item_name = OmsDbItems.Order
        self._engine = engine

    def check(self, data: List[Order]):
        data.sort(key=lambda x: x.CreateTime)
        if len(data) == 0:
            self.engine.logger.info('最新Order, None, OrderBook is empty')
        else:
            latest_order = data[-1]
            self.engine.logger.info(f'最新Order, '
                             f'{latest_order.CreateTime.strftime("%Y-%m-%d %H:%M:%S")}, '
                             f'{latest_order.Trader}, '
                             f'{latest_order.Ticker}, '
                             f'{str(Direction(latest_order.Direction).name)}, '
                             f'{latest_order.Volume}, {latest_order.LimitPrice}')

    def renew(self):
        pass


class OmsOrderBookUnfilledOrderChecker(OmsCheckerBase):
    # Str_Subprocess_Run_SendWarning = 'call ./_OrderBookCheckedUnfilledOrderWarning.bat & exit'
    """ 长时间未成交。需要2次确认，因为可能是因为oms db更新缓慢导致的"""

    def __init__(self, engine: OmsCheckerEngine, unfilled_order_warning_gap=60, output_root=''):
        super().__init__()
        self.item_name = OmsDbItems.Order
        self._engine = engine

        # 将n秒前的未成交订单认定为 _unfilled_order
        self._unfilled_order_checking_gap = float(unfilled_order_warning_gap)
        # 已经检查过的 未成交订单。避免重复报错。
        self._checked_unfilled_orders: List[Order] = []
        self._checked_unfilled_orders_warned: List[Order] = []
        if output_root:
            self.output_file = os.path.join(output_root, 'Checking_UnfilledOrders.csv')
            if not os.path.isdir(output_root):
                os.makedirs(output_root)
        else:
            self.output_file = ''

    # 检查OrderBook
    def check(self, data: List[Order]):
        # 检查是否有 未成交订单
        l_unfilled_order: List[Order] = []
        for order in data:
            if order.OrderStatus not in [4, 5]:
                if (datetime.now() - order.CreateTime).seconds >= self._unfilled_order_checking_gap:
                    l_unfilled_order.append(order)

        # 重新检查 order状态
        self._checked_unfilled_orders: List[Order] = [_ for _ in self._checked_unfilled_orders if _ in l_unfilled_order]
        for order in self._checked_unfilled_orders:
            self.engine._db.session.refresh(order)
        self._checked_unfilled_orders: List[Order] = [_ for _ in self._checked_unfilled_orders if _.OrderStatus not in [4 ,5]]

        # 文件输出检查结果，未成交订单
        if self.output_file and self._checked_unfilled_orders:
            l_s_output = []
            s_dt_now = datetime.now().strftime("%Y%m%d %H%M%S")
            for unfilled_order in self._checked_unfilled_orders:
                l_s_output.append(
                    f'{s_dt_now},'
                    f'{unfilled_order.Trader},'
                    f'{unfilled_order.Account},'
                    f'{unfilled_order.Ticker},'
                    f'{str(unfilled_order.Direction)},'
                    f'{str(unfilled_order.Volume)},'
                    f'{unfilled_order.CreateTime.strftime("%Y%m%d %H%M%S")},'
                    f'{str(unfilled_order)}')
            with open(self.output_file, 'a+') as f:
                f.writelines('\n'.join(l_s_output) + '\n')

        # 未曾报错的order
        l_unfilled_order_unwarned = [
            _ for _ in self._checked_unfilled_orders if _ not in self._checked_unfilled_orders_warned]

        # 发送检查结果
        # subprocess.run(self.Str_Subprocess_Run_SendWarning, shell=True, stdout=subprocess.PIPE)
        # subprocess.Popen("call _SendWarning.bat", shell=True, stdout=None)
        # os.system('start /B "./_SendWarning.bat"')
        # os.system('ResultWarning.py -f "./Output/Warning_PreloadFirstSignal.csv" --mswarning -t')

        # 弹框报错【新增的】未成交订单
        if l_unfilled_order_unwarned:
            for order in l_unfilled_order_unwarned:
                self.engine.logger.warning(
                    f'此订单长时间未成交或撤单, '
                    f'{order.Trader}, {order.Ticker}, '
                    f'{str(Direction(order.Direction).name)}, '
                    f'{str(order.Volume)}, {str(order.LimitPrice)}, '
                    f'{order.CreateTime.strftime("%Y-%m-%d %H:%M:%S")}'
                )
                self._checked_unfilled_orders_warned.append(order)
            # 弹框报警
            self.engine.send_warning('订单长时间未成交')

        #
        self._checked_unfilled_orders = l_unfilled_order

    def renew(self):
        self._checked_unfilled_orders_warned = []
        if self.output_file:
            with open(self.output_file, 'w') as f:
                pass


class OmsOrderBookRepeatedOrderChecker(OmsCheckerBase):
    """
    反复挂单， 一般是反复挂撤单。
    eg: 一分钟内，同一标的挂单量超过10此
    """

    def __init__(self, engine: OmsCheckerEngine, checking_time=60, max_order=10, output_root=''):
        super().__init__()
        self.item_name = OmsDbItems.Order
        self._engine = engine

        # 同一标的，_checking_time秒内下单次数大于_max_order次
        self._checking_time = float(checking_time)
        self._max_order = float(max_order)
        #
        self._last_checked_time = datetime.now()
        self._last_checked_order: Order or None = None
        self._last_warning_order = []
        # 输出检查结果
        if output_root:
            self.output_file = os.path.join(output_root, 'Checking_RepeatedOrders.csv')
            if not os.path.isdir(output_root):
                os.makedirs(output_root)
        else:
            self.output_file = ''

    def check(self, data: List[Order]):
        # 筛选所需要检查的数据
        latest_order = max(data, key=lambda x: x.CreateTime)
        if latest_order == self._last_checked_order:
            # 没有新数据
            return
        checking_data = [order for order in data if order.CreateTime >
                         (latest_order.CreateTime - timedelta(seconds=self._checking_time))]
        # 按 account ticker分组
        d_account_ticker_order = defaultdict(lambda: defaultdict(list))
        for order in checking_data:
            d_account_ticker_order[order.Account][order.Ticker].append(order)
        l_warning_infos: List[dict] = []
        for account in d_account_ticker_order.keys():
            for ticker, orders in d_account_ticker_order[account].items():
                if len(orders) < self._max_order:
                    continue
                else:
                    _first_order: Order = sorted(orders, key=lambda x: x.CreateTime)[0]
                    _warning_info = {
                        "Trader": _first_order.Trader,
                        "Account": _first_order.Account,
                        "Ticker": _first_order.Ticker,
                        "OrdersCount": len(orders),
                    }
                    l_warning_infos.append(_warning_info)
                    if _warning_info not in self._last_warning_order:
                        self.engine.send_warning(f'{_first_order.Trader.split("@")[1]}, {str(ticker)}, 高频挂单')

        # 输出检查结果
        if self.output_file and l_warning_infos:
            l_s_output = []
            s_dt_now = datetime.now().strftime("%Y%m%d %H%M%S")
            for _ in l_warning_infos:
                l_s_output.append(
                    f'{s_dt_now},'
                    f'{_["Trader"]},'
                    f'{_["Account"]},'
                    f'{_["Ticker"]},'
                    f'{str(_["OrdersCount"])}'
                )
            with open(self.output_file, 'a+') as f:
                f.writelines('\n'.join(l_s_output) + '\n')
        self._last_warning_order = l_warning_infos

    def renew(self):
        if self.output_file:
            with open(self.output_file, 'w') as f:
                pass


class OmsTradeBookDataDownloadOnlyChecker(OmsCheckerBase):
    def __init__(self):
        self._engine = ''
        self.item_name = OmsDbItems.Trade

    def check(self, data):
        pass

    def renew(self):
        pass


class OmsPositionBookDataDownloadOnlyChecker(OmsCheckerBase):
    def __init__(self):
        self._engine = ''
        self.item_name = OmsDbItems.Position

    def check(self, data):
        pass

    def renew(self):
        pass
