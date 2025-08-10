import pandas as pd
import os


def save_dataframe_to_excel(df, file_path, sheet_name='Sheet1', index=False):
    """
    将DataFrame数据保存到Excel文件

    参数:
    df (pd.DataFrame): 要保存的数据框
    file_path (str): 保存文件的路径
    sheet_name (str): Excel工作表名称，默认为'Sheet1'
    index (bool): 是否保存索引，默认为False

    返回:
    bool: 保存成功返回True，失败返回False
    """
    try:
        # 检查输入是否为DataFrame
        if not isinstance(df, pd.DataFrame):
            raise TypeError("输入数据不是pandas DataFrame类型")

        # 检查文件路径是否有效
        if not file_path:
            raise ValueError("文件路径不能为空")

        # 获取目录路径并检查是否存在
        dir_path = os.path.dirname(file_path)
        if dir_path and not os.path.exists(dir_path):
            # 创建目录
            os.makedirs(dir_path, exist_ok=True)
            print(f"已创建目录: {dir_path}")

        # 检查文件扩展名
        if not file_path.endswith(('.xlsx', '.xls')):
            file_path += '.xlsx'
            print(f"文件扩展名自动更正为: {file_path}")

        # 保存DataFrame到Excel
        df.to_excel(file_path, sheet_name=sheet_name, index=index)

        # 验证文件是否已创建
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            print(f"数据成功保存到: {os.path.abspath(file_path)}")
            return True
        else:
            raise IOError("文件创建失败或为空文件")

    except TypeError as te:
        print(f"类型错误: {str(te)}")
    except ValueError as ve:
        print(f"值错误: {str(ve)}")
    except PermissionError:
        print(f"权限错误: 没有权限写入文件 {file_path}")
    except IOError as ioe:
        print(f"IO错误: {str(ioe)}")
    except Exception as e:
        print(f"保存文件时发生未知错误: {str(e)}")

    return False


# 示例用法
if __name__ == "__main__":
    try:
        # 创建示例DataFrame
        data = {
            '姓名': ['张三', '李四', '王五'],
            '年龄': [25, 30, 35],
            '城市': ['北京', '上海', '广州']
        }
        df = pd.DataFrame(data)

        # 保存到Excel
        save_success = save_dataframe_to_excel(
            df=df,
            file_path='data/人员信息表',  # 会自动添加.xlsx扩展名
            sheet_name='员工信息'
        )

        if not save_success:
            print("数据保存失败")

    except Exception as e:
        print(f"示例运行出错: {str(e)}")
