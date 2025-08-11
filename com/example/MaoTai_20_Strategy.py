import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import os

# 设置中文显示
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
plt.rcParams["axes.unicode_minus"] = False  # 解决负号显示问题


class MA20Strategy:
    def __init__(self, data_path=None):
        """初始化策略"""
        self.data = None  # 存储股票数据
        self.results = None  # 存储回测结果
        self.data_path = data_path

    # 修改load_data方法以支持Excel文件和特殊日期格式
    def load_data(self, data_path=None):
        """加载股票数据，支持Excel文件和20150101格式日期"""
        try:
            # 如果提供了新的路径则使用新路径，否则使用初始化时的路径
            path = data_path if data_path else self.data_path

            if not path or not os.path.exists(path):
                raise FileNotFoundError("数据文件不存在")

            # 根据文件扩展名选择合适的读取方法
            if path.endswith(('.xlsx', '.xls')):
                # 读取Excel文件，尝试不同的引擎处理可能的格式问题
                try:
                    self.data = pd.read_excel(
                        path,
                        parse_dates=['trade_date'],  # 尝试自动解析日期
#                        engine='openpyxl'  # 用于处理.xlsx文件
                    )
                except:
                    # 备用引擎
                    self.data = pd.read_excel(
                        path,
                        parse_dates=['trade_date'],
#                        engine='xlrd'  # 用于处理旧版.xls文件
                    )
            else:
                # 读取CSV文件，尝试不同编码
                encodings = ['utf-8', 'gbk', 'gb2312', 'utf-16']
                for encoding in encodings:
                    try:
                        self.data = pd.read_csv(
                            path,
                            parse_dates=['trade_date'],
                            encoding=encoding
                        )
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    raise UnicodeDecodeError("无法解析文件编码，请检查文件格式")

            # 处理20150101格式的日期（整数或字符串）
            if not pd.api.types.is_datetime64_any_dtype(self.data['trade_date']):
                # 尝试将日期列转换为字符串，再转换为 datetime
                self.data['trade_date'] = pd.to_datetime(
                    self.data['trade_date'].astype(str),
                    format='%Y%m%d',
                    errors='coerce'
                )

                # 检查是否有无法转换的日期
                invalid_dates = self.data['trade_date'].isna().sum()
                if invalid_dates > 0:
                    print(f"警告：有 {invalid_dates} 个日期格式无效，已转换为NaT")
                    # 移除无效日期的行
                    self.data = self.data.dropna(subset=['trade_date'])

            # 将日期设为索引并排序
            self.data = self.data.set_index('trade_date').sort_index()

            print(f"数据加载成功，共 {len(self.data)} 条记录")
            print(f"数据时间范围: {self.data.index[0].date()} 至 {self.data.index[-1].date()}")
            return True

        except Exception as e:
            print(f"数据加载失败: {str(e)}")
            return False

    def generate_signals(self):
        """生成交易信号"""
        if self.data is None:
            print("请先加载数据")
            return False

        try:
            # 计算20日均线
            self.data['MA20'] = self.data['close'].rolling(window=20).mean()

            # 生成信号：价格上穿20日均线时买入(1)，下穿时卖出(-1)，无信号(0)
            self.data['signal'] = 0

            # 价格上穿20日均线
            self.data.loc[self.data['close'] > self.data['MA20'], 'signal'] = 1

            # 价格下穿20日均线
            self.data.loc[self.data['close'] < self.data['MA20'], 'signal'] = -1

            # 确保信号只在交叉点变化时产生
            self.data['position'] = self.data['signal'].diff()

            print("交易信号生成成功")
            return True

        except Exception as e:
            print(f"生成信号失败: {str(e)}")
            return False

    def backtest(self, initial_capital=1000000):
        """回测策略"""
        if self.data is None or 'position' not in self.data.columns:
            print("请先加载数据并生成交易信号")
            return False

        try:
            # 复制数据用于回测计算
            backtest_data = self.data.copy()

            # 初始化资金和持仓
            backtest_data['cash'] = initial_capital
            backtest_data['shares'] = 0  # 持有股份数量
            backtest_data['total_assets'] = initial_capital  # 总资产

            current_cash = initial_capital
            current_shares = 0

            # 遍历每个交易日执行策略
            for i, date in enumerate(backtest_data.index):
                close_price = backtest_data.loc[date, 'close']
                position = backtest_data.loc[date, 'position']

                # 买入信号
                if position == 2:  # 从-1变为1，实际差值为2
                    if current_cash > 0:
                        # 计算可购买的最大股数（假设整手买卖，1手=100股）
                        max_shares = (current_cash // (close_price * 100)) * 100
                        if max_shares > 0:
                            current_shares += max_shares
                            current_cash -= max_shares * close_price
                            print(
                                f"{date.date()}: 买入 {max_shares} 股，价格 {close_price:.2f}，剩余资金 {current_cash:.2f}")

                # 卖出信号
                elif position == -2:  # 从1变为-1，实际差值为-2
                    if current_shares > 0:
                        current_cash += current_shares * close_price
                        print(
                            f"{date.date()}: 卖出 {current_shares} 股，价格 {close_price:.2f}，总资金 {current_cash:.2f}")
                        current_shares = 0

                # 更新资产数据
                backtest_data.loc[date, 'cash'] = current_cash
                backtest_data.loc[date, 'shares'] = current_shares
                backtest_data.loc[date, 'total_assets'] = current_cash + current_shares * close_price

            self.results = backtest_data
            print("回测完成")
            return True

        except Exception as e:
            print(f"回测失败: {str(e)}")
            return False

    def analyze_results(self):
        """分析回测结果"""
        if self.results is None:
            print("请先进行回测")
            return

        # 计算策略表现
        initial_assets = self.results['total_assets'].iloc[0]
        final_assets = self.results['total_assets'].iloc[-1]
        total_return = (final_assets - initial_assets) / initial_assets * 100

        # 计算持有期间
        start_date = self.results.index[0].date()
        end_date = self.results.index[-1].date()
        days_held = (end_date - start_date).days
        years_held = days_held / 365.25

        # 计算年化收益率
        annual_return = ((1 + total_return / 100) ** (1 / years_held) - 1) * 100 if years_held > 0 else 0

        # 计算买入持有策略收益
        buy_hold_return = (self.results['close'].iloc[-1] - self.results['close'].iloc[0]) / self.results['close'].iloc[
            0] * 100

        # 统计交易次数
        # 使用pandas的sum()方法，确保在Series上操作
        trade_signals = self.results['position'].abs() == 2
        total_trades = trade_signals.sum(axis=0)  # 明确指定轴

        # 确保比较操作返回的是布尔型Series
        buy_signals = (self.results['position'] == 2).sum(axis=0)
        sell_signals = (self.results['position'] == -2).sum(axis=0)


        print("\n===== 策略表现分析 =====")
        print(f"回测时间段: {start_date} 至 {end_date}")
        print(f"初始资金: {initial_assets:.2f} 元")
        print(f"最终资产: {final_assets:.2f} 元")
        print(f"总收益率: {total_return:.2f}%")
        print(f"年化收益率: {annual_return:.2f}%")
        print(f"买入持有策略收益率: {buy_hold_return:.2f}%")
        print(f"总交易次数: {total_trades} 次 (买入: {buy_signals} 次, 卖出: {sell_signals} 次)")

    def plot_results(self):
        """可视化策略结果"""
        if self.results is None:
            print("请先进行回测")
            return

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12), sharex=True)

        # 第一个子图：价格和均线
        ax1.plot(self.results.index, self.results['close'], label='收盘价', linewidth=2)
        ax1.plot(self.results.index, self.results['MA20'], label='20日均线', linewidth=2, color='orange')

        # 标记买入卖出点
        buy_signals = self.results[self.results['position'] == 2]
        sell_signals = self.results[self.results['position'] == -2]

        ax1.scatter(buy_signals.index, buy_signals['close'], marker='^', color='g', label='买入', s=100)
        ax1.scatter(sell_signals.index, sell_signals['close'], marker='v', color='r', label='卖出', s=100)

        ax1.set_title('茅台股价与20日均线策略')
        ax1.set_ylabel('价格 (元)')
        ax1.legend()
        ax1.grid(True)

        # 第二个子图：资产变化
        ax2.plot(self.results.index, self.results['total_assets'], label='策略资产', linewidth=2)
        ax2.axhline(y=self.results['total_assets'].iloc[0], color='r', linestyle='--', label='初始资金')

        ax2.set_title('策略资产变化')
        ax2.set_xlabel('日期')
        ax2.set_ylabel('资产 (元)')
        ax2.legend()
        ax2.grid(True)

        # 设置x轴日期格式
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.xticks(rotation=45)

        plt.tight_layout()
        plt.show()


# 示例用法
if __name__ == "__main__":
    # 创建策略实例
    strategy = MA20Strategy()

    # 加载数据（这里使用模拟数据，实际使用时替换为真实数据路径）
    # 如果没有真实数据，可以将下面的generate_sample_data设为True生成模拟数据
    generate_sample_data = False

    if generate_sample_data:
        # 生成模拟的茅台股票数据（2020-2023年）
        dates = pd.date_range(start='2015-01-15', end='2023-12-31', freq='B')  # 工作日
        np.random.seed(42)  # 固定随机种子，使结果可重复

        # 生成模拟价格（基于茅台大致价格范围）
        base_price = 149.36
        price_changes = np.random.normal(0, 20, len(dates))
        close_prices = base_price + np.cumsum(price_changes)

        # 确保价格为正数
        close_prices = np.maximum(close_prices, 500)

        # 生成其他价格数据
        open_prices = close_prices * (1 + np.random.normal(0, 0.01, len(dates)))
        high_prices = np.maximum(close_prices, open_prices) * (1 + np.random.normal(0, 0.02, len(dates)))
        low_prices = np.minimum(close_prices, open_prices) * (1 - np.random.normal(0, 0.02, len(dates)))
        volumes = np.random.randint(1000000, 5000000, len(dates))

        # 创建DataFrame
        sample_data = pd.DataFrame({
            'date': dates,
            'open': open_prices,
            'high': high_prices,
            'low': low_prices,
            'close': close_prices,
            'volume': volumes
        })

        # 保存为CSV（实际使用时替换为真实数据）
        sample_data.to_csv('data/maotai_sample_data.csv', index=False)
        print("已生成模拟数据并保存为 data/maotai_sample_data.csv")

        # 加载模拟数据
        strategy.load_data('data/maotai_sample_data.csv')
    else:
        # 加载真实数据（请替换为你的数据路径）
        strategy.load_data('data/maotai_daily_20150101.xlsx')

    # 生成交易信号
    strategy.generate_signals()

    # 执行回测（初始资金100万）
    strategy.backtest(initial_capital=1000000)

    # 分析结果
    strategy.analyze_results()

    # 可视化结果
    strategy.plot_results()


