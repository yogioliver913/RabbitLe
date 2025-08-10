import tushare as ts
from com.example import Tusharetoken
from com.example.tools import Df_To_Excel as dte
import numpy as np
import pandas as pd

# Tushare Pro配置（A股数据）
ts_token = Tusharetoken.get()  # 在tushare.pro官网注册获取
ts.set_token(ts_token)
pro = ts.pro_api()


def refresh_maotai_data():
    df = ts.pro_bar(ts_code='600519.SH', adj='qfq', start_date='20150101', end_date='20250731')
    dte.save_dataframe_to_excel(df=df,file_path='data/茅台前复权日线',  # 会自动添加.xlsx扩展名
            sheet_name='茅台前复权日线')



'''
长期投资者：优先选择夏普比率>1.5 且 索提诺比率>2.0的标的，辅以最大回撤<20%验证。
保守型投资者：索提诺比率>2.5的资产（如黄金、稳健型CTA策略）更适配 。
动态调整：牛市中可容忍稍低夏普（>1.2），熊市中需严守索提诺>1.0的底线。
'''
def calc_maotai_data():
    df = pd.read_excel('data/茅台前复权日线.xlsx', sheet_name='茅台前复权日线')  # 指定工作表
    # 假设df为DataFrame，含日期(date)和日收益率(pct_chg)
    # df_sorted = df.sort_values(by='列名', ascending=True)

    df = df.head()

    # 日累计回报率
    df['cumulative_return'] = (1 + df['pct_chg']/100).cumprod()
    print(df['cumulative_return'])

    # 日累计回报率的最大值
    df['peak'] = df['cumulative_return'].cummax()
    print(df['peak'])

    # 日回撤百分比序列
    df['drawdown'] = 1 - df['cumulative_return'] / df['peak']
    print(df['drawdown'])

    # 日最大回撤百分比
    max_drawdown = df['drawdown'].max()
    print(max_drawdown)

    # 夏普比率
    sharpe_ratio = ((df['pct_chg']/100).mean() * 252) / ((df['pct_chg']/100).std() * np.sqrt(252))
    print(sharpe_ratio)

    # 索提诺比率
    downside_returns = df[df['pct_chg']/100 < 0]['pct_chg']/100
    print(downside_returns)
    sortino_ratio = ((df['pct_chg']/100).mean() * 252) / (downside_returns.std() * np.sqrt(252))
    print(sortino_ratio)

    # 假设df包含净值序列'net_value'
    drawdown_start = df[df['drawdown'] > 0].index[0]  # 回撤起点
    drawdown_end = df[df['cumulative_return'] >= df.loc[drawdown_start, 'peak']].index[0]  # 修复终点
    max_duration = (drawdown_end - drawdown_start).days
    print(max_duration)

    # 自由现金流收益率


    return max_drawdown, sharpe_ratio, downside_returns, sortino_ratio, max_duration


if __name__ == '__main__':
    max_drawdown, sharpe_ratio, downside_returns, sortino_ratio, max_duration = calc_maotai_data()
    # print(max_drawdown)
    # print(sharpe_ratio)
    # print(downside_returns)
    # print(sortino_ratio)
