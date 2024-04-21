# coding=utf-8

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import talib

from tumbler.object import BarData, FactorData
from tumbler.constant import Interval
from tumbler.service.mysql_service import MysqlService

use_start_time = datetime(2017, 1, 1)
use_end_time = datetime.now() + timedelta(hours=10)


def create_df():
    global use_start_time, use_end_time
    mysql_service_manager = MysqlService.get_mysql_service()
    symbols = mysql_service_manager.get_mysql_distinct_symbol(table=MysqlService.get_kline_table(Interval.DAY.value))

    bars = mysql_service_manager.get_bars(symbols=[], period=Interval.DAY.value,
                                          start_datetime=use_start_time,
                                          end_datetime=use_end_time,
                                          sort_way="symbol")

    bars = BarData.suffix_filter(bars, suffix="_usdt")
    bars.sort()

    pd_data = BarData.get_pandas_from_bars(bars)
    pd_data = pd_data.set_index(["symbol", "datetime"]).sort_index().reset_index()

    return pd_data


def merge_factor(df, factor_codes):
    global use_start_time, use_end_time
    if isinstance(factor_codes, str):
        factor_codes = [factor_codes]
    mysql_service_manager = MysqlService.get_mysql_service()
    factor_ret = mysql_service_manager.get_factors(
        factor_codes=factor_codes,
        interval=Interval.DAY.value,
        start_dt=use_start_time,
        end_dt=use_end_time
    )
    for factor_code in factor_codes:
        factor_df = FactorData.get_factor_df(factor_ret, factor_code)
        df = pd.merge(df, factor_df, how='left', left_on=['symbol', 'datetime'], right_on=['symbol', 'datetime'])

        # rank_code = f"rank_{factor_code}"
        # df[rank_code] = df.groupby(by=['datetime']).apply(
        #     lambda x: pd.DataFrame(pd.qcut(df[factor_code], [0, 0.2, 0.4, 0.6, 0.8, 1.0]
        #                                    , labels=[rank_code + "_1", rank_code + "_2",
        #                                              rank_code + "_3", rank_code + "_4",
        #                                              rank_code + "_5"
        #                                              ]), index=x.index))

    return df


def make_feature(df):
    factor_codes = []
    factor_codes += ["droc5", "droc10", "droc20", "droc30", "droc60", "droc90"]
    factor_codes += ["er10", "er20", "er30", "er60", "er90", "er120"]
    factor_codes += ["der10", "der20", "der30", "der60", "der90", "der120"]
    factor_codes += ["roc1", "roc3", "roc5", "roc10", "roc20", "roc30", "roc60", "roc90", "roc120"]
    factor_codes += ["size"]

    df = merge_factor(df, factor_codes)
    return df


def make_target(df, num_day_rise=1):
    df["target"] = df.groupby(by=['symbol']).apply(
        lambda x: pd.DataFrame(pd.Series(talib.ROC(x.close, num_day_rise)).shift(-1 * num_day_rise), index=x.index)
    )
    return df


def split_train_test(df, split_datetime=datetime(2021, 5, 1)):
    train_df = df[df.datetime <= split_datetime].copy()
    test_df = df[df.datetime > split_datetime].copy()

    return train_df, test_df


df = create_df()
df.to_csv("df.csv")


df = merge_factor(df, ["roc30", "roc60"])
df.to_csv("new_df.csv")
print(df)
#
df = make_target(df, num_day_rise=2)
df.to_csv("target_df.csv")
print(df)

df = df.dropna()

train, test = split_train_test(df)
train.to_csv("train.csv")
test.to_csv("test.csv")


from sklearn.model_selection import KFold
import lightgbm as lgb

seed0 = 2021
params0 = {
    'objective': 'rmse',
    'boosting_type': 'gbdt',
    'max_depth': -1,
    'max_bin': 100,
    'min_data_in_leaf': 500,
    'learning_rate': 0.05,
    'subsample': 0.72,
    'subsample_freq': 4,
    'feature_fraction': 0.5,
    'lambda_l1': 0.5,
    'lambda_l2': 1.0,
    'categorical_column': [0],
    'seed': seed0,
    'feature_fraction_seed': seed0,
    'bagging_seed': seed0,
    'drop_seed': seed0,
    'data_random_seed': seed0,
    'n_jobs': -1,
    'verbose': -1}
seed1 = 42


# Function to early stop with root mean squared percentage error
def rmspe(y_true, y_pred):
    return np.sqrt(np.mean(np.square((y_true - y_pred) / y_true)))


def feval_rmspe(y_pred, lgb_train):
    y_true = lgb_train.get_label()
    return 'RMSPE', rmspe(y_true, y_pred), False


def train_and_evaluate_lgb(train, test, params):
    features = [col for col in train.columns if col not in {"datetime", "target", "exchange", "symbol"}]
    y = train['target']
    # Create out of folds array
    oof_predictions = np.zeros(train.shape[0])
    # Create test array to store predictions
    test_predictions = np.zeros(test.shape[0])
    # Create a KFold object
    kfold = KFold(n_splits=5, random_state=2021, shuffle=True)
    # Iterate through each fold
    for fold, (trn_ind, val_ind) in enumerate(kfold.split(train)):
        print(f'Training fold {fold + 1}')
        x_train, x_val = train.iloc[trn_ind], train.iloc[val_ind]
        y_train, y_val = y.iloc[trn_ind], y.iloc[val_ind]
        # Root mean squared percentage error weights
        train_weights = 1 / np.square(y_train)
        val_weights = 1 / np.square(y_val)
        train_dataset = lgb.Dataset(x_train[features], y_train, weight=train_weights)
        val_dataset = lgb.Dataset(x_val[features], y_val, weight=val_weights)
        model = lgb.train(params=params,
                          num_boost_round=1000,
                          train_set=train_dataset,
                          valid_sets=[train_dataset, val_dataset],
                          verbose_eval=250,
                          early_stopping_rounds=50,
                          feval=feval_rmspe)
        # Add predictions to the out of folds array
        oof_predictions[val_ind] = model.predict(x_val[features])
        # Predict the test set
        test_predictions += model.predict(test[features]) / 5
    rmspe_score = rmspe(y, oof_predictions)
    print(f'Our out of folds RMSPE is {rmspe_score}')
    lgb.plot_importance(model, max_num_features=20)
    # Return test predictions
    return test_predictions


# Traing and evaluate
predictions_lgb = train_and_evaluate_lgb(train, test, params0)
test['target'] = predictions_lgb
