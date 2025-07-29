


def updateNew():




    df = pro.daily_info(start_date='20160101', end_date='20250729',ts_code='SH_MARKET',exchange='SH',
                        fields='trade_date,ts_code,ts_name,com_count,total_mv,float_mv,amount,pe')
    total_SH_val = df[df['ts_code'] == 'SH_MARKET'].iloc[0].total_mv
    df.to_excel('SH_MARKET_20160101_20250630.xlsx', index=False)

    df = pro.daily_info(start_date='20160101', end_date='20250729',ts_code='SZ_MARKET',exchange='SZ',
                        fields='trade_date,ts_code,ts_name,total_mv,float_mv,amount,ts_name,pe')
    total_SZ_val = df[df['ts_code'] == 'SZ_MARKET'].iloc[0].total_mv
    df.to_excel('SZ_MARKET_20160101_20250630.xlsx', index=False)



