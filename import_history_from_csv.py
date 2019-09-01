"""
导入从网易财经下载的个股历史数据
"""
import os
import csv
import pymysql
import db_pool
import logging


def get_files_list(path):
    """
    获取path下的所有文件，并返回文件的相对路径
    :param path:
    :return:
    """
    files_list = os.listdir(path)
    path_list = []
    for file in files_list:
        # dl_datapath / 0600121 - 19000101 - 20190313.csv
        path_list.append(path + "/" + file)
    return path_list


def read_csv(file_path):
    """
    读取csv文件，并以二维数组的形式返回
    :param file_path:
    :return:
    """
    with open(file_path, 'r', encoding='gbk') as f:
        data = csv.reader(f)
        dataset = []
        for line in data:
            dataset.append(line)
        f.close()

    return dataset

def insert_into_mysql(dataset,db_p):
    """
    数据导入MySQL的hostory_data表中
    :param dataset:
    :return:
    """
    # 删除表头
    del (dataset[0])

    # 建立数据库连接
    conn = db_p.connection()
    cur = conn.cursor()

    # 对每行数据进行处理
    data_sum = len(dataset)
    tp_sum = 0
    err_sum = 0

    for line in dataset:
        while len(line) < 15:
            line.append('Null')
        if line[8] != "None":
            line[1] = line[1][1:]
            # 将缺失数据的Null替换为数据库可识别的Null
            line = ['Null' if x == 'None' else x for x in line]
            value_str = ""
            # 为时间、股票代码和股票名称添加引号
            for i in range(0, 3):
                line[i] = "'" + line[i] + "'"
            # 拼接SQL
            for item in line:
                value_str = value_str + item + ", "
                # value_str = value_str + "'" + item + "', "
            sql = "insert into history_price values ({});".format(value_str[:-2])
            # print(sql)

            try:
                cur.execute(sql)
            except pymysql.Error as e:
                err_sum = err_sum + 1
                print('ERROR: {}'.format(e))
                logging.warning('ERROR: {}'.format(e))
                print(sql)
                logging.warning(sql)
        else:
            tp_sum += 1
    print('----summary------')
    logging.info('----summary------')
    print('total: {}    success: {}    err: {}    tp: {}'.format(data_sum, (data_sum-tp_sum-err_sum), err_sum, tp_sum))
    logging.info('total: {}    success: {}    err: {}    tp: {}'.format(data_sum, (data_sum-tp_sum-err_sum), err_sum, tp_sum))

    conn.commit()
    cur.close()
    conn.close()
    return [data_sum, err_sum, tp_sum]


def check_data(dataset):
    """
    检查文件是否为空表或仅有表头
    检查数据的表头是否符合要求，防止数据录入错误的行
    :param dataset:
    :return:
    """
    if len(dataset) < 2:
        print("check data Error. Empty set.")
        logging.warning("check data Error. Empty set.")
        return False
    if len(dataset[0]) == 15:
        key = ['日期', '股票代码', '名称', '收盘价', '最高价', '最低价', '开盘价', '前收盘', '涨跌额', '涨跌幅', '换手率', '成交量', '成交金额', '总市值', '流通市值']
        check = True
        for i in range(0, 15):
            if key[i] != dataset[0][i]:
                check = False
        if check:
            print("check data OK.")
            logging.info("check data OK.")
            return True
        else:
            print("something wrong with data.")
            logging.warning("something wrong with data.")
            return False
    else:
        print("check data Error. Wrong header.")
        logging.warning("check data Error. Wrong header.")
        return False


def remove_file(path):
    """
    删除文件
    :param path:
    :return:
    """
    print("finish processing {}, remove.".format(path))
    logging.info("finish processing {}, remove.".format(path))
    os.remove(path)


def get_count_history(db_p):
    """
    获取数据库中股票历史数据的总条数
    :param db_p:
    :return:
    """
    conn = db_p.connection()
    cur = conn.cursor()
    sql = "select count(*) from history_price;"
    res = 0
    try:
        cur.execute(sql)
        result = cur.fetchone()
        res = result[0]
    except pymysql.Error as e:
        print("Error(get_count_history): {}".format(e))
        logging.warning("Error(get_count_history): {}".format(e))
    return res


def run():
    # 初始化Logging
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',
                        level=logging.DEBUG, filemode='a',
                        filename='import_history_from_csv.log')
    # 初始化连接池
    db_p = db_pool.get_db_pool(False)

    # 获取文件列表
    path = "dl_datapath"
    files_list = get_files_list(path)

    # 初始化计数器
    count_all = 0
    count_err = 0
    count_tp = 0
    task_count = len(files_list)
    check_count = 0
    finish_count = 0
    # 获取数据库初始信息条数
    initial_count = get_count_history(db_p)
    for file_path in files_list:
        print("task_count: {}   finish: {}  todo: {}".format(task_count, finish_count, (task_count-finish_count)))
        logging.info("task_count: {}   finish: {}  todo: {}".format(task_count, finish_count, (task_count-finish_count)))
        print("finish_count: {} err: {} tp: {}  total: {}".format(count_all, count_err, count_tp, check_count))
        logging.info("finish_count: {} err: {} tp: {}  total: {}".format(count_all, count_err, count_tp, check_count))
        print("processing file: {}".format(file_path))
        logging.info("processing file: {}".format(file_path))
        dataset = read_csv(file_path)
        if check_data(dataset):     # 如果数据列正确
            # 插入数据库
            feedback = insert_into_mysql(dataset, db_p)
            count_all += feedback[0]
            count_err += feedback[1]
            count_tp += feedback[2]
            # 检查数据库记录条数是否正确
            check_count = get_count_history(db_p)
            if check_count != (initial_count + count_all - count_err - count_tp):
                print("Count number don't match! the file is: {}".format(file_path))
                logging.warning("Count number don't match! the file is: {}".format(file_path))
                break
            # 删除数据文件
            remove_file(file_path)
            finish_count += 1
        else:       # 记录错误的数据文件信息

            pass


if __name__ == '__main__':
    run()

