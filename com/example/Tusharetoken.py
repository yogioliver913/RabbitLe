import os


def get():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, '../../config/tushare_key')
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
        if content is not None:
            return content
        else:
            raise Exception