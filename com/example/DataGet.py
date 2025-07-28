import yfinance as yf
import tushare as ts
import pandas as pd
import matplotlib.pyplot as plt
import schedule
from pandas_datareader import data as pdr

# Tushare Pro配置（A股数据）
ts_token = ''  # 在tushare.pro官网注册获取
ts.set_token(ts_token)
pro = ts.pro_api()

# Yahoo Finance配置（美股数据）
SP500_TICKER = "^GSPC"  # 标普500指数
US10Y_TICKER = "^TNX"   # 10年期美债收益率


def get_buffett_index(market="A"):
    """计算巴菲特指数（总市值/GDP）"""
    if market == "A":
        # 获取A股总市值（沪深两市）
        # 获取深圳和上海市场20200320各板块交易指定字段的数据
        df = pro.daily_info(trade_date='20250724', exchange='SZ,SH', fields='trade_date,ts_code,ts_name,total_mv,float_mv,amount,ts_name,pe')
        total_mkt_val = df.iloc[0].total_mv

        # 获取中国最新GDP（年度数据）
        gdp_data = pro.cn_gdp()  # 从Tushare获取GDP
        latest_gdp = gdp_data.iloc[0].gdp * 1e8  # 单位：亿元[2](@ref)

        buffett_ratio = total_mkt_val / latest_gdp
        return buffett_ratio * 100  # 转换为百分比

    elif market == "US":
        # 美股总市值（Wilshire 5000指数）
        wilshire = yf.Ticker("^W5000")
        mkt_val = wilshire.fast_info.market_cap  # 单位：美元

        # 美国GDP（FRED数据）
        gdp_us = pdr.get_data_fred("GDP", start="2025-01-01").iloc[-1].values[0] * 1e9  # 单位：美元[2](@ref)

        return (mkt_val / gdp_us) * 100

def calc_equity_bond_yield(market="A"):
    """计算股债收益率差（股票市盈率倒数 - 债券收益率）"""
    if market == "A":
        # A股沪深300盈利率
        hs300_pe = pro.index_daily(ts_code="000300.SH").iloc[0].pe  # 沪深300PE[4](@ref)
        equity_yield = (1 / hs300_pe) * 100  # 转换为收益率%

        # 中国10年期国债收益率
        bond_yield = pro.bond_yield(ts_code="010107.IB").iloc[0].yield_  # 国债代码示例

        return equity_yield - bond_yield

    elif market == "US":
        # 标普500盈利率
        sp500 = yf.Ticker(SP500_TICKER)
        pe_ratio = sp500.info["trailingPE"]
        equity_yield = (1 / pe_ratio) * 100

        # 美国10年期国债收益率
        bond_yield = yf.Ticker(US10Y_TICKER).history(period="1d").Close.iloc[-1]

        return equity_yield - bond_yield



def generate_valuation_report():
    """生成估值报告并绘图"""
    # 计算关键指标
    buffett_cn = get_buffett_index("A")
    buffett_us = get_buffett_index("US")
    yield_gap_cn = calc_equity_bond_yield("A")
    yield_gap_us = calc_equity_bond_yield("US")

    # 巴菲特指数信号判断[2,3](@ref)
    buffett_signal_cn = "低估" if buffett_cn < 75 else ("高估" if buffett_cn > 120 else "合理")
    buffett_signal_us = "低估" if buffett_us < 75 else ("高估" if buffett_us > 120 else "合理")

    # 股债收益差信号（历史分位）
    yield_signal_cn = "股票占优" if yield_gap_cn > 1.5 else "债券占优"
    yield_signal_us = "股票占优" if yield_gap_us > 1.0 else "债券占优"

    # 绘制结果
    fig, ax = plt.subplots(2, 1, figsize=(12, 10))

    # 巴菲特指数图表
    ax[0].bar(["A股", "美股"], [buffett_cn, buffett_us], color=["#FF6B6B" if buffett_cn>120 else "#4ECDC4",
                                                                "#FF6B6B" if buffett_us>120 else "#4ECDC4"])
    ax[0].axhline(y=75, color='grey', linestyle='--')
    ax[0].axhline(y=120, color='red', linestyle='--')
    ax[0].set_title(f"巴菲特指数 (A股: {buffett_cn:.1f}% [{buffett_signal_cn}], 美股: {buffett_us:.1f}% [{buffett_signal_us}])")

    # 股债收益差图表
    ax[1].bar(["A股", "美股"], [yield_gap_cn, yield_gap_us],
              color=["#1A535C" if yield_gap_cn>0 else "#FF6B6B", "#1A535C" if yield_gap_us>0 else "#FF6B6B"])
    ax[1].axhline(y=0, color='black')
    ax[1].set_title(f"股债收益率差 (A股: {yield_gap_cn:.2f}% [{yield_signal_cn}], 美股: {yield_gap_us:.2f}% [{yield_signal_us}])")

    plt.tight_layout()
    plt.savefig("valuation_report.png")
    return fig





def daily_job():
    report = generate_valuation_report()
    report.savefig(f"reports/{pd.Timestamp.now().strftime('%Y%m%d')}.png")


if __name__ == '__main__':
    #data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    #data.head()

    # 生成报告
    report = generate_valuation_report()
    report.show()
    #schedule.every().day.at("18:00").do(daily_job)
