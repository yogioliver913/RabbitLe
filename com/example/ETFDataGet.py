from com.example import Tusharetoken
import tushare as ts
import pandas as pd
from datetime import datetime


TOKEN = Tusharetoken.get()
ts.set_token(TOKEN)
pro = ts.pro_api()


def get_etf_data():
    # 定义参数
    etf_list = ['513530.SH', '159545.SZ']
    start_date = '20200101'
    end_date = '20250731'
    output_file = 'data/etf_history_adj.xlsx'

    # 创建Excel写入器
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        for ts_code in etf_list:
            try:
                print(f"正在处理 {ts_code} 数据...")

                # 获取日线数据
                daily_df = pro.fund_daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    fields='ts_code,trade_date,open,high,low,close,vol,amount'
                )

                # 数据清洗
                daily_df['trade_date'] = pd.to_datetime(daily_df['trade_date'])
                daily_df = daily_df.sort_values('trade_date')

                # 获取复权因子
                adj_df = pro.fund_adj(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date
                )

                # 数据清洗
                adj_df['trade_date'] = pd.to_datetime(adj_df['trade_date'])
                adj_df = adj_df.sort_values('trade_date')

                # 合并数据
                merged_df = pd.merge(
                    daily_df[['trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']],
                    adj_df[['trade_date', 'adj_factor']],
                    on='trade_date',
                    how='left'
                )

                # 前复权计算（以end_date为基准）
                base_date = datetime.strptime(end_date, '%Y%m%d')
                base_adj = merged_df[merged_df['trade_date'] == base_date]['adj_factor'].values[0]

                # 计算复权价格
                price_cols = ['open', 'high', 'low', 'close']
                for col in price_cols:
                    merged_df[col] = merged_df[col] * (merged_df['adj_factor'] / base_adj)

                # 保留两位小数
                merged_df[price_cols] = merged_df[price_cols].round(2)
                merged_df['vol'] = merged_df['vol'].astype(int)
                merged_df['amount'] = merged_df['amount'].astype(int)

                # 保存到Excel
                sheet_name = ts_code.split('.')[0]  # 提取代码前缀作为sheet名
                merged_df.to_excel(writer, sheet_name=sheet_name, index=False)

                print(f"{ts_code} 数据处理完成，共 {len(merged_df)} 条记录")

            except Exception as e:
                print(f"处理 {ts_code} 时发生错误: {str(e)}")
                continue

if __name__ == "__main__":
    # 创建输出目录
    import os

    os.makedirs('data', exist_ok=True)

    get_etf_data()
    print("所有数据处理完成，文件已保存至 data/etf_history_adj.xlsx")
