"""
本文档用于读取并导入个股换手率表到数据库
"""
import db_pool
import time

# 初始化数据库连接
db = db_pool.get_db_pool(False)
conn = db.connection()
cursor = conn.cursor()

def is_valid_date(strdate):
    """
    判断是否为日期
    :param strdate:
    :return:
    """
    try:
        time.strptime(strdate, "%Y-%m-%d")
        return True
    except:
        return False


with open('large_dataset/gghslb.txt', encoding='gbk') as f:
    line_count = 0
    data_success = 0
    data_error = 0
    data_jumpover = 0
    for line in f:
        line_count += 1
        if not is_valid_date(line.split('\t')[0]):  # 判断该行是否为标题行，如果是则跳过
            print('LINE ERROR: {}'.format(line))
            data_jumpover += 1
            continue
        trimed_line = "('" + line.strip().replace('\t', "', '") + "'); "
        sql = "insert into gghslb values {}".format(trimed_line)
        # print(sql)
        try:
            cursor.execute(sql)
            data_success += 1
        except:
            print('ERROR: {}'.format(sql))
            data_error += 1

        if line_count%10000 == 0:
            conn.commit()
            print("{} lines ({}%) finished.".format(line_count, round(line_count/71682527*100, 2)))

# 关闭数据库连接
conn.commit()
cursor.close()
conn.close()

print("===== SUMMMARY =====")
print("line_count = {}".format(line_count))
print("data_success = {}".format(data_success))
print("data_error = {}".format(data_error))
print("data_jumpover = {}".format(data_jumpover))



