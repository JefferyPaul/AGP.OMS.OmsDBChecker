# OMSDbChecker

检查Oms数据库，判断OMS是否正常运行，BM是否正常下单  

1. 查看最新order的更新时间。不会报错，用于观察oms是否正常运行
2. 检查是否有长时间未成交的订单，超时报错

## 注意

- _SendWarning.bat 中必须使用 --nopause，如果不使用，在报错后，此线程将会暂停，而 OMSDbChecker 在进入“非交易时间”时需要等待此线程结束，就会使得整个 OMSDbChecker 在此处阻塞。

## 参数

- loop_interval，每次检查的时间间隔。
- unfilled_order_warning_gap, 若当前时间 - 订单时间 > 此值，认定为未成交订单。


## 运行

### OmsDBChecker

- OmsCheckerEngine(ScheduleRunner)：继承ScheduleRunner，按规定时间段运行；连接OmsDb；周期性地运行 checker.check()。
- OmsOrderBookRunningChecker(Checker)：
