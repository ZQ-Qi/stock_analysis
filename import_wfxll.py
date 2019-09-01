"""
将无风险利率导入三因子模型和五因子模型数据表中
"""
import csv
import db_pool


def get_csv_data(path):
    """
    从路径下获取csv文件，并对其去除表头和简单计算
    0列：日期
    1列：年化无风险利率
    2列：日化无风险利率
    :param path:
    :return:
    """
    dataset = []
    with open(path, 'r', encoding='gbk') as f:
        data = csv.reader(f)
        for line in data:
            dataset.append([line[0], line[1].strip()])
        f.close()
        del(dataset[0])
        for line in dataset:
            line[1] = float(line[1])        # str -> float
            line.append(line[1]/360)        # 日化
    return dataset


def import_to_db(data, db):
    """
    对该日期前无风险利率为空的更新对应的无风险利率
    :param data:
    :param db:
    :return:
    """
    conn = db.connection()
    cur = conn.cursor()
    date = data[0]
    wfxll = data[2]
    sql = "update syzmx set wfxll = {} where wfxll is null and dt < '{}';".format(wfxll, date)
    cur.execute(sql)
    sql = "update wyzmx set wfxll = {} where wfxll is null and dt < '{}';".format(wfxll, date)
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    path = 'origin_data/wfxll.csv'
    data = get_csv_data(path)       # 获取无风险利率列表
    db = db_pool.get_db_pool(False)     # 初始化数据库连接池
    for item in data:
        print("update wfxll as {} before {}".format(item[2], item[0]))
        import_to_db(item, db)

