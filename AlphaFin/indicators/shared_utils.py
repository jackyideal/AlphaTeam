"""
共享工具模块 - Tushare初始化、数据库连接、公共绘图函数
"""
import matplotlib
matplotlib.use('Agg')  # 必须在 pyplot 导入之前设置

import warnings
warnings.filterwarnings('ignore')

matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'Heiti TC']
matplotlib.rcParams['axes.unicode_minus'] = False
matplotlib.rcParams['figure.facecolor'] = 'white'

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import tushare as ts
from sqlalchemy import create_engine
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from AlphaFin.config import TUSHARE_TOKEN, DB_ROOT, BASE_DIR

pro = ts.pro_api(TUSHARE_TOKEN)


def get_engine(db_name):
    """创建SQLite数据库连接"""
    path = f'sqlite:////{DB_ROOT}/{db_name}'
    return create_engine(path)


def to_dt_index(df, col='trade_date'):
    """将 DataFrame 按 trade_date 排序并设为 datetime 索引（同时删除该列避免歧义）"""
    df = df.sort_values(col).reset_index(drop=True).copy()
    df.index = pd.to_datetime(df[col])
    df = df.drop(columns=[col])
    return df


def add_stat_lines(ax, series, colors=('gray', 'blue'), std_mult=1.0, label_prefix=''):
    """在坐标轴上添加均值和标准差参考线"""
    mean = series.mean()
    std = series.std()
    ax.axhline(y=mean, color=colors[0], linestyle='--', linewidth=1,
               label=f'{label_prefix}均值', alpha=0.8)
    ax.axhline(y=mean + std_mult * std, color=colors[1], linestyle='--', linewidth=1,
               label=f'{label_prefix}均值+{std_mult}σ', alpha=0.7)
    ax.axhline(y=mean - std_mult * std, color=colors[1], linestyle='--', linewidth=1,
               label=f'{label_prefix}均值-{std_mult}σ', alpha=0.7)


def get_index_daily(ts_code='000001.SH', start_date='20150101'):
    """获取指数日线数据（datetime 索引，无 trade_date 列）"""
    df = pro.index_daily(ts_code=ts_code, start_date=start_date,
                         fields='trade_date,close,pct_chg,vol,amount')
    return to_dt_index(df)


def fig_style(fig, ax, title='', xlabel='', ylabel_left='', ylabel_right='', ax2=None):
    """统一图表样式"""
    ax.set_title(title, fontsize=22, pad=12)
    ax.set_xlabel(xlabel, fontsize=14)
    ax.set_ylabel(ylabel_left, fontsize=16)
    if ax2 is not None:
        ax2.set_ylabel(ylabel_right, fontsize=16)
    ax.tick_params(labelsize=12)
    ax.grid(alpha=0.3)
    fig.tight_layout()


def dual_axis_plot(data_left, data_right, label_left, label_right,
                   title='', figsize=(20, 8), color_left='black', color_right='orange',
                   add_stats_right=True):
    """通用双轴图表生成器"""
    fig, ax1 = plt.subplots(figsize=figsize, facecolor='white')
    ax1.plot(data_left.index, data_left.values, color=color_left, linewidth=2, label=label_left)
    ax1.set_ylabel(label_left, fontsize=16)
    ax1.legend(loc='upper left', fontsize=12)
    ax1.tick_params(labelsize=12)

    ax2 = ax1.twinx()
    ax2.plot(data_right.index, data_right.values, color=color_right, linewidth=1.5, label=label_right)
    if add_stats_right:
        add_stat_lines(ax2, data_right.dropna())
    ax2.set_ylabel(label_right, fontsize=16)
    ax2.legend(loc='upper right', fontsize=12)
    ax2.tick_params(labelsize=12)

    ax1.set_title(title, fontsize=22, pad=12)
    ax1.grid(alpha=0.3)
    fig.tight_layout()
    return fig
