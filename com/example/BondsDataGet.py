import tushare as ts
import pandas as pd
import os
import time
from datetime import datetime, timedelta
from com.example import Tusharetoken

# 设置Tushare Token（替换为您的实际Token）
TOKEN = Tusharetoken.get()
ts.set_token(TOKEN)
pro = ts.pro_api()


File_Path='data/bond_yields.xlsx'
Start_Date = '20160101'


def fetch_bond_yields(start_date, end_date):
    """
    获取指定日期范围内的国债收益率数据（修复列名重复问题）
    """
    terms = [1, 3, 5, 10]
    final_df = pd.DataFrame()  # 初始化最终结果DataFrame

    # 分割日期范围（每次不超过2000天）
    start_dt = datetime.strptime(start_date, '%Y%m%d')
    end_dt = datetime.strptime(end_date, '%Y%m%d')
    date_ranges = []
    current_date = start_dt
    while current_date <= end_dt:
        next_date = current_date + timedelta(days=1999)
        if next_date > end_dt:
            next_date = end_dt
        date_ranges.append((
            current_date.strftime('%Y%m%d'),
            next_date.strftime('%Y%m%d')
        ))
        current_date = next_date + timedelta(days=1)

    # 遍历每个期限，独立获取数据并重命名列
    for term_idx, term in enumerate(terms):
        term_data = pd.DataFrame()
        for range_idx, (range_start, range_end) in enumerate(date_ranges):
            try:
                # 获取单期限数据
                df = pro.yc_cb(
                    ts_code='1001.CB',
                    curve_type='1',  # 即期收益率
                    start_date=range_start,
                    end_date=range_end,
                    curve_term=term
                )
                # 重命名列：直接使用期限作为列名后缀
                df = df[['trade_date', 'yield']].rename(columns={'yield': f'{term}Y_YTM'})

                # 合并当前期限的子区间数据
                if term_data.empty:
                    term_data = df
                else:
                    term_data = pd.concat([term_data, df], ignore_index=True)

                # 控制调用频率（每分钟不超过2次）
                time.sleep(31)
            except Exception as e:
                print(f"获取{term}年期数据出错({range_start}至{range_end}): {e}")

        # 将当前期限的数据合并到最终结果
        if final_df.empty:
            final_df = term_data
        else:
            final_df = pd.merge(final_df, term_data, on='trade_date', how='outer')  # 按日期外连接

    # 按日期排序并重置索引
    final_df = final_df.sort_values('trade_date').reset_index(drop=True)
    return final_df


def update_bond_yields(file_path=File_Path):
    """
    更新数据到Excel文件（列名格式已修复）
    """
    # 确定起始日期（若文件存在则续更，否则从2016-01-01开始）
    if os.path.exists(file_path):
        existing_df = pd.read_excel(file_path)
        last_date = existing_df['trade_date'].max()
        last_date_dt = datetime.strptime(str(last_date), '%Y%m%d')
        start_date = (last_date_dt + timedelta(days=1)).strftime('%Y%m%d')
    else:
        start_date = Start_Date
        existing_df = pd.DataFrame()

    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    if start_date > end_date:
        print("数据已是最新，无需更新")
        return existing_df

    print(f"正在拉取数据: {start_date} 至 {end_date}")
    new_df = fetch_bond_yields(start_date, end_date)

    if new_df.empty:
        print("未获取到新数据")
        return existing_df if not existing_df.empty else pd.DataFrame()

    # 合并新旧数据
    updated_df = pd.concat([existing_df, new_df], ignore_index=True) if not existing_df.empty else new_df
    updated_df.to_excel(file_path, index=False)
    print(f"数据已保存至 {file_path}, 共 {len(updated_df)} 条记录")
    return updated_df


if __name__ == "__main__":
    df = update_bond_yields()
    if not df.empty:
        print("\n最新5条数据（列名已修复）:")
        print(df[['trade_date', '1Y_YTM', '3Y_YTM', '5Y_YTM', '10Y_YTM']].tail())