import tushare as ts
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from com.example import Tusharetoken
import os


# 配置项
class Config:
    # 基本面筛选标准
    # TODO 目前只考虑了年报表，没有考虑季度报表，以及财报更新
    筛选标准 = {
        # 盈利能力
        'roe': {'min': 15, 'years': 5},  # 连续5年ROE≥15%
        'roic': {'min': 10, 'years': 5},  # 连续5年ROIC≥10%
        'grossprofit_margin': {'min': 30, 'years': 5},  # 连续5年毛利率≥30%
        'netprofit_margin': {'min': 8, 'years': 5},  # 连续5年净利率≥8%

        # 财务健康
        'debt_to_asset': {'max': 60},  # 资产负债率≤60%
        'current_ratio': {'min': 1.5},  # 流动比率≥1.5
        'quick_ratio': {'min': 1.0},  # 速动比率≥1.0
        'cash_flow_to_profit': {'min': 0.8},  # 经营现金流/净利润≥80%

        # 成长稳定性
        'revenue_growth': {'min': 8, 'years': 3},  # 近3年营收复合增速≥8%
        'profit_growth': {'min': 10, 'years': 3},  # 近3年净利润复合增速≥10%

        # 估值指标
        'pe': {'max': 20, 'percentile': 20},  # PE≤20倍且处于近5年20%分位以下
        'pb': {'max': 2.5, 'percentile': 25},  # PB≤2.5倍且处于近5年25%分位以下
        'dividend_rate': {'min': 2.5}  # 股息率≥2.5%
    }

    # 日期设置
    # 当前日期 = datetime.now().strftime('%Y%m%d')
    当前日期 = '20250731'
    开始日期 = (datetime.now() - timedelta(days=5 * 366)).strftime('%Y%m%d')  # 5年前
    三年前日期 = (datetime.now() - timedelta(days=3 * 366)).strftime('%Y%m%d')  # 3年前


class TushareData:
    def __init__(self, token):
        """初始化Tushare接口"""
        ts.set_token(token)
        self.pro = ts.pro_api()
        self.a500_stocks = None

    def get_a500_stocks(self):
        """获取A500指数成分股列表"""
        if self.a500_stocks is None:
            # 注意：Tushare可能没有直接的A500成分股接口，这里使用中证500作为替代
            # TODO 粗选股票池待确定
            # 实际应用中可能需要从其他渠道获取A500成分股列表
            index_stocks = self.pro.index_weight(index_code='000905.SH', trade_date=Config.当前日期)
            self.a500_stocks = index_stocks['con_code'].tolist()
        return self.a500_stocks

    def get_latest_financial_data(self, ts_code):
        """获取最新财务指标数据"""
        try:
            # 获取最新的财务指标
            tmp_fina_indicator = self.pro.query('fina_indicator', ts_code=ts_code,
                                            start_date=Config.开始日期, end_date=Config.当前日期)
            fina_indicator = self._filter_last_day_of_year(tmp_fina_indicator)


            # 获取利润表数据
            income = self.pro.income(ts_code=ts_code, start_date=Config.开始日期,
                                     end_date=Config.当前日期,
                                     fields='ts_code,end_date,report_type,basic_eps,gross_profit_rate,net_profit_rate')

            # 获取资产负债表数据
            balancesheet = self.pro.balancesheet(ts_code=ts_code, start_date=Config.开始日期,
                                                 end_date=Config.当前日期,
                                                 fields='ts_code,end_date,report_type,debt_to_asset,current_ratio,quick_ratio')

            # 获取现金流量表数据
            cashflow = self.pro.cashflow(ts_code=ts_code, start_date=Config.开始日期,
                                         end_date=Config.当前日期,
                                         fields='ts_code,end_date,report_type,net_cash_flows_oper_act,net_profit')

            # 获取估值数据
            valuation = self.pro.daily_basic(ts_code=ts_code,trade_date=Config.当前日期,
                                       fields='ts_code,pe,pe_ttm,pb,ps,dv_ratio')

            return {
                'fina_indicator': fina_indicator,
                'income': income,
                'balancesheet': balancesheet,
                'cashflow': cashflow,
                'valuation': valuation
            }
        except Exception as e:
            print(f"获取{ts_code}财务数据出错: {e}")
            return None

    def get_stock_basic_info(self, ts_code):
        """获取股票基本信息"""
        try:
            basic = self.pro.stock_basic(ts_code=ts_code, fields='ts_code,name,industry')
            return basic.iloc[0] if not basic.empty else None
        except Exception as e:
            print(f"获取{ts_code}基本信息出错: {e}")
            return None

    def _get_last_year_last_day(self):
        """获取上一年最后一天，返回格式为YYYYMMDD的字符串"""
        last_year = datetime.now().year - 1
        return datetime(last_year, 12, 31).strftime("%Y%m%d")

    def _filter_last_day_of_year(self, df):
        """
        筛选出end_date为每年最后一天（12月31日）的行
        :param df: 包含end_date列的DataFrame
        :return: 筛选后的DataFrame
        """
        # 检查end_date列是否存在
        if 'end_date' not in df.columns:
            raise ValueError("DataFrame中必须包含'end_date'列")

        # 筛选出end_date以'1231'结尾的行（即12月31日）
        filtered_df = df[df['end_date'].str.endswith('1231')]

        return filtered_df


class StockFilter:
    def __init__(self, data_provider):
        self.data_provider = data_provider

    def filter_stocks(self):
        """筛选符合条件的股票"""
        a500_stocks = self.data_provider.get_a500_stocks()
        if not a500_stocks:
            print("未能获取A500成分股列表")
            return []

        result = []
        total = len(a500_stocks)

        for i, ts_code in enumerate(a500_stocks):
            print(f"正在处理 {i + 1}/{total}: {ts_code}")

            # 获取股票基本信息
            basic_info = self.data_provider.get_stock_basic_info(ts_code)
            if not basic_info.any():
                continue

            # 获取财务数据
            financial_data = self.data_provider.get_latest_financial_data(ts_code)
            if not financial_data:
                continue

            # 检查是否符合所有筛选条件
            if self._check_all_conditions(ts_code, financial_data):
                # 收集符合条件的股票信息
                stock_info = self._collect_stock_info(ts_code, basic_info, financial_data)
                result.append(stock_info)

        return result

    def _check_all_conditions(self, ts_code, financial_data):
        """检查是否符合所有筛选条件"""
        try:
            # 检查盈利能力指标
            if not self._check_profitability(financial_data):
                return False

            # 检查财务健康指标
            if not self._check_financial_health(financial_data):
                return False

            # 检查成长稳定性指标
            if not self._check_growth_stability(financial_data):
                return False

            # 检查估值指标
            if not self._check_valuation(financial_data):
                return False

            return True
        except Exception as e:
            print(f"检查{ts_code}条件时出错: {e}")
            return False

    def _check_profitability(self, financial_data):
        """检查盈利能力指标"""
        fina_indicator = financial_data['fina_indicator']
        income = financial_data['income']

        if len(fina_indicator) < Config.筛选标准['roe']['years']:
            return False

        # 按年份排序
        annual_reports = fina_indicator.sort_values('end_date', ascending=False)

        # 检查连续5年ROE
        for i in range(Config.筛选标准['roe']['years']):
            if annual_reports.iloc[i]['roe'] < Config.筛选标准['roe']['min']:
                return False

        # 检查连续5年ROIC
        for i in range(Config.筛选标准['roic']['years']):
            if annual_reports.iloc[i]['roic'] < Config.筛选标准['roic']['min']:
                return False

        # 检查毛利率和净利率
        annual_income = income.sort_values('end_date', ascending=False)
        if len(annual_income) < Config.筛选标准['grossprofit_margin']['years']:
            return False

        for i in range(Config.筛选标准['grossprofit_margin']['years']):
            if annual_income.iloc[i]['grossprofit_margin'] < Config.筛选标准['grossprofit_margin']['min']:
                return False

        for i in range(Config.筛选标准['netprofit_margin']['years']):
            if annual_income.iloc[i]['netprofit_margin'] < Config.筛选标准['netprofit_margin']['min']:
                return False

        return True

    def _check_financial_health(self, financial_data):
        """检查财务健康指标"""
        balancesheet = financial_data['balancesheet']
        cashflow = financial_data['cashflow']

        # 获取最新年报
        latest_bs = balancesheet[balancesheet['report_type'] == 1].sort_values('end_date', ascending=False).iloc[0]
        latest_cf = cashflow[cashflow['report_type'] == 1].sort_values('end_date', ascending=False).iloc[0]

        # 资产负债率
        if latest_bs['debt_to_asset'] > Config.筛选标准['debt_to_asset']['max']:
            return False

        # 流动比率
        if latest_bs['current_ratio'] < Config.筛选标准['current_ratio']['min']:
            return False

        # 速动比率
        if latest_bs['quick_ratio'] < Config.筛选标准['quick_ratio']['min']:
            return False

        # 经营现金流与净利润比
        # TODO 这里的“经营现金流” 采用的是“间接法经营现金流”-im_net_cashflow_oper_act
        if latest_cf['im_net_cashflow_oper_act'] / latest_cf['net_profit'] < Config.筛选标准['cash_flow_to_profit'][
            'min']:
            return False

        return True

    def _check_growth_stability(self, financial_data):
        """检查成长稳定性指标"""
        fina_indicator = financial_data['fina_indicator']

        # 获取最近3年的年报
        # annual_reports = fina_indicator[fina_indicator['report_type'] == 1].sort_values('end_date', ascending=False)
        annual_reports = fina_indicator
        if len(annual_reports) < Config.筛选标准['revenue_growth']['years']:
            return False

        # 计算营收复合增长率
        revenues = annual_reports['revenue_ps'].head(Config.筛选标准['revenue_growth']['years']).values
        revenue_growth = (revenues[0] / revenues[-1]) ** (1 / 3) - 1
        if revenue_growth * 100 < Config.筛选标准['revenue_growth']['min']:
            return False

        # 计算净利润复合增长率
        # TODO profit_dedt 扣除非经常性损益后的净利润（扣非净利润）
        profit_dedt = annual_reports['profit_dedt'].head(Config.筛选标准['profit_growth']['years']).values
        profit_growth = (profit_dedt[0] / profit_dedt[-1]) ** (1 / 3) - 1
        if profit_growth * 100 < Config.筛选标准['profit_growth']['min']:
            return False

        return True

    def _check_valuation(self, financial_data):
        """检查估值指标"""
        valuation = financial_data['valuation']
        if valuation.empty:
            return False

        latest_valuation = valuation.iloc[0]

        # 检查市盈率
        if latest_valuation['pe_ttm'] > Config.筛选标准['pe']['max']:
            return False

        # 检查市净率
        if latest_valuation['pb'] > Config.筛选标准['pb']['max']:
            return False

        # 检查股息率
        if latest_valuation['dv_ttm'] < Config.筛选标准['dividend_rate']['min']:
            return False

        return True

    def _collect_stock_info(self, ts_code, basic_info, financial_data):
        """收集股票信息和指标值"""
        fina_indicator = financial_data['fina_indicator']
        income = financial_data['income']
        balancesheet = financial_data['balancesheet']
        cashflow = financial_data['cashflow']
        valuation = financial_data['valuation']

        # 获取最新年报数据
        latest_annual = \
        fina_indicator[fina_indicator['report_type'] == 1].sort_values('end_date', ascending=False).iloc[0]
        latest_valuation = valuation.iloc[0]

        return {
            '股票代码': ts_code,
            '股票名称': basic_info['name'],
            '行业': basic_info['industry'],
            '最新年报日期': latest_annual['end_date'],
            # 盈利能力指标
            'ROE(%)': latest_annual['roe'],
            'ROIC(%)': latest_annual['roic'],
            '毛利率(%)': income[income['report_type'] == 1].sort_values('end_date', ascending=False).iloc[0][
                'gross_profit_rate'],
            '净利率(%)': income[income['report_type'] == 1].sort_values('end_date', ascending=False).iloc[0][
                'net_profit_rate'],
            # 财务健康指标
            '资产负债率(%)':
                balancesheet[balancesheet['report_type'] == 1].sort_values('end_date', ascending=False).iloc[0][
                    'debt_to_asset'],
            '流动比率': balancesheet[balancesheet['report_type'] == 1].sort_values('end_date', ascending=False).iloc[0][
                'current_ratio'],
            '速动比率': balancesheet[balancesheet['report_type'] == 1].sort_values('end_date', ascending=False).iloc[0][
                'quick_ratio'],
            '经营现金流/净利润':
                cashflow[cashflow['report_type'] == 1].sort_values('end_date', ascending=False).iloc[0][
                    'net_cash_flows_oper_act'] /
                cashflow[cashflow['report_type'] == 1].sort_values('end_date', ascending=False).iloc[0]['net_profit'],
            # 成长指标
            '近3年营收复合增速(%)': self._calculate_growth_rate(fina_indicator, 'operate_rev', 3),
            '近3年净利润复合增速(%)': self._calculate_growth_rate(fina_indicator, 'net_profit', 3),
            # 估值指标
            '市盈率(PE)': latest_valuation['pe_ttm'],
            '市净率(PB)': latest_valuation['pb'],
            '股息率(%)': latest_valuation['dividend_rate']
        }

    def _calculate_growth_rate(self, fina_indicator, column, years):
        """计算复合增长率"""
        annual_data = fina_indicator[fina_indicator['report_type'] == 1].sort_values('end_date', ascending=False)
        if len(annual_data) < years:
            return None

        values = annual_data[column].head(years).values
        return ((values[0] / values[-1]) ** (1 / years) - 1) * 100


class ExcelExporter:
    @staticmethod
    def export_to_excel(stocks, filename=None):
        """将筛选结果导出到Excel"""
        if not stocks:
            print("没有符合条件的股票")
            return

        if not filename:
            filename = f"value_investing_screening_results_{datetime.now().strftime('%Y%m%d')}.xlsx"

        # 创建DataFrame
        df = pd.DataFrame(stocks)

        # 调整列顺序
        columns_order = [
            '股票代码', '股票名称', '行业', '最新年报日期',
            'ROE(%)', 'ROIC(%)', '毛利率(%)', '净利率(%)',
            '资产负债率(%)', '流动比率', '速动比率', '经营现金流/净利润',
            '近3年营收复合增速(%)', '近3年净利润复合增速(%)',
            '市盈率(PE)', '市净率(PB)', '股息率(%)'
        ]
        df = df[columns_order]

        # 导出到Excel
        try:
            df.to_excel(filename, index=False)
            print(f"筛选结果已导出到 {os.path.abspath(filename)}")
            print(f"共找到 {len(stocks)} 只符合条件的股票")
        except Exception as e:
            print(f"导出Excel出错: {e}")


def main():
    # 请替换为您的Tushare token
    TUSHARE_TOKEN = Tusharetoken.get()

    # 初始化数据提供者
    data_provider = TushareData(TUSHARE_TOKEN)

    # 初始化筛选器
    filter = StockFilter(data_provider)

    # 执行筛选
    print("开始筛选符合条件的股票...")
    qualified_stocks = filter.filter_stocks()

    # 导出结果
    ExcelExporter.export_to_excel(qualified_stocks)


if __name__ == "__main__":
    main()
