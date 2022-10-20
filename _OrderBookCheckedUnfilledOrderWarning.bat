chcp 65001
@echo off

cd %~dp0
python ResultWarning.py -f "./Output_OrderBooks/UnfilledOrderBooks.csv" --mswarning --mswarning_timestamp --nopause

