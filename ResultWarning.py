import os
import sys
from datetime import datetime
import argparse
import json

PATH_ROOT = os.path.abspath(os.path.dirname(__file__))
PATH_CONFIG_ROOT = os.path.join(PATH_ROOT, 'Config')
sys.path.append(PATH_ROOT)

from helper.simpleLogger import MyLogger
my_logger = MyLogger(name='ResultWarning', output_root=os.path.join(PATH_ROOT, 'logs'))
try:
    from helper.emailHelper import send_email
except :
    pass
try:
    from helper.PyMessageClient import MessageClient
except :
    pass
try:
    from helper.tp_WarningBoard import run_warning_board
except :
    pass

arg_parser = argparse.ArgumentParser()
# 报错信息 的文件
arg_parser.add_argument('-f', '--files', nargs="*", help='报错信息文件')
# 弹窗提示
arg_parser.add_argument('--warningboardstring', default="")
# 发送邮件提醒
arg_parser.add_argument('--emailwarning', action='store_true', default=False)
# 报送 报错信息 至 MessageServer 
arg_parser.add_argument('--mswarning', action='store_true', default=False)
# ms value是否带时间错
arg_parser.add_argument('--mswarning_timestamp', action='store_true', default=False)
# 默认出错会暂停
arg_parser.add_argument('--nopause', action='store_true', default=False)

args = arg_parser.parse_args()
_warning_files: list = list(args.files)
if not len(_warning_files) >= 1:
    my_logger.error('请输入检查文件')
    os.system('pause')
    raise Exception
_warning_board_string = args.warningboardstring
_is_emailwarning = args.emailwarning
_is_mswarning = args.mswarning
_is_mswarning_timestamp = args.mswarning_timestamp
_no_pause = args.nopause

"""
定义方法
"""


# 邮件
def _email_warning():
    # my_logger.info(f'inEmailWarning')
    config_file = os.path.join(PATH_CONFIG_ROOT, 'EmailWarningConfig.json')
    d_config = json.loads(open(config_file, encoding='utf-8').read())

    if IS_ERROR:
        _text = '\tWARNING\n\t错误行数: %s' % (str(len(l_warning_msg)))
    else:
        _text = '正常'
    send_email(
        # subject=d_config["subject"],
        text=_text,
        files=_warning_files,
        **d_config
    )


#　MSReport
def _ms_warning():
    # my_logger.info(f'inMSWarning')
    config_file = os.path.join(PATH_CONFIG_ROOT, 'MSWarningConfig.json')
    d_config = json.loads(open(config_file, encoding='utf-8').read(),)

    if IS_ERROR:
        msg = d_config['warning_value']
        if _is_mswarning_timestamp:
            msg += '.%s' % datetime.now().strftime('%Y/%m/%d %H:%M:%S')
        MessageClient(
            ip=d_config['ip'], port=d_config['port']).sendmessage(
            key=d_config['key'], message=msg)
    else:
        msg = 'Checked'
        if _is_mswarning_timestamp:
            msg += '.%s' % datetime.now().strftime('%Y/%m/%d %H:%M:%S')
        MessageClient(
            ip=d_config['ip'], port=d_config['port'], logger=my_logger).sendmessage(
            key=d_config['key'], message=msg)


# warning board
def _board_warning():
    # my_logger.info(f'inBoardWarning')

    if IS_ERROR:
        run_warning_board(_warning_board_string)


# 脚本暂停
def _pause_warning():
    # my_logger.info(f'inPauseWarning')

    if IS_ERROR:
        os.system('pause')

    
# 读取文件
l_warning_msg = []
for _warning_file in _warning_files:
    _warning_file = os.path.abspath(_warning_file)
    assert os.path.isfile(_warning_file)
    # my_logger.info(f'Reading file, {_warning_file}')
    try:
        with open(_warning_file, encoding='utf-8') as f:
            _msg = f.readlines()
    except:        
        with open(_warning_file, encoding='gb2312') as f:
            _msg = f.readlines()
    _msg = [_.strip() for _ in _msg if _.strip()]
    l_warning_msg += _msg

# 判断是否正常
IS_ERROR = len(l_warning_msg) != 0

# 报警

if IS_ERROR:
    my_logger.info('发现异常')
else:
    my_logger.info('正常,未发现错误信息')

if _is_emailwarning:
    _email_warning()
if _is_mswarning:
    _ms_warning()
if _warning_board_string:
    _board_warning()        
# 暂停脚本，必须最后运行
if not _no_pause:
    _pause_warning()

