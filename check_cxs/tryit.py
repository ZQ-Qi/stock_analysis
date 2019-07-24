"""没有用"""

import csv

namechange_file = 'cxs_namechange_list.csv'

namechange_list = []
with open(namechange_file, 'r', encoding='gbk') as f:
    f_context = f.read()
    f_lines = f_context.split('\n')
    f_lines = f_lines[:-1]
    for f_line in f_lines:
        line = f_line.split(',')
        sorted_line = [i for i in line if i != '']
        if len(sorted_line) > 1:
            namechange_list.append(tuple(sorted_line))
    namechange_list = tuple(namechange_list)
for i in namechange_list:
    print(i)


def double_index(dd_tuple, search_str):
    """
    分析字符串在二维元组中的位置，返回第一维和第二维坐标，若查找不到则返回[-1，-1]
    :param dd_tuple:
    :param search_str:
    :return:
    """
    for i, i_tuple in enumerate(dd_tuple):
        if search_str in i_tuple:
            j = i_tuple.index(search_str)
            return [i, j]
    return [-1, -1]


print(double_index(namechange_list, '中信证券股份有限公司'))





