# coding=utf-8

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import talib

from tumbler.object import BarData, FactorData
from tumbler.constant import Interval
from tumbler.service.mysql_service import MysqlService

frame = pd.DataFrame({'data1': np.random.randn(1000),
                      'data2': np.random.randn(1000)})
print(frame)

quariles = pd.cut(frame.data1, 4)
# print(quariles[:10])
# print(quariles)

quriles_1 = pd.qcut(frame.data1, 4)
# print(quriles_1)
# print(quriles_1[:10])

# data = pd.Series([0, 8, 1, 5, 3, 7, 2, 6, 10, 4, 9])
# print(pd.qcut(data, [0, 0.5, 1], labels=['small number', 'large number']))

from sklearn.model_selection import KFold

X = np.array([[1, 2], [3, 4], [1, 2], [3, 4]])
y = np.array([1, 2, 3, 4])
kf = KFold(n_splits=2)
print(kf.get_n_splits(X))

for k,(train,test) in enumerate(kf.split(x,y)):
	print (k,(train,test))
	x_train=X.iloc[train]
	x_test=X.iloc[test]
	y_train=Y.iloc[train]
	y_test=Y.iloc[tes]