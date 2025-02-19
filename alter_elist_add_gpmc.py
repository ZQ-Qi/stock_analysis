"""
为event_list补充股票名称列的数据
"""
import db_pool


def get_gpmc(db_p, gpdm, dt, isipo):
    conn = db_p.connection()
    cur = conn.cursor()
    if isipo == 0:      # 如果非IPO事件，从增发实施表中进行查询
        sql = "select gpmc from zfss where gpdm = '{}' and ssggr = '{}';".format(gpdm, dt)
    else:       # IPO事件从新股发行资料表中获取
        sql = "select gpmc from xgfxzl where gpdm = '{}' and ssrq = '{}';".format(gpdm, dt)
    cur.execute(sql)
    result = []
    for item in cur.fetchall():
        result.append(item[0])
    cur.close()
    conn.close()
    if len(result) == 1:
        return result[0]
    elif len(result) > 1:  # 如果得到多于1个的发行方式，判断发行方式是否一致
        flag = True
        for i in range(1, len(result)):
            if result[0] != result[i]:
                flag = False
        if flag:    # 如果两次或两次同时发行，发行方式一致则返回一致的发行方式
            return result[0]
        else:    # 两次或多次发行方式不一致
            print("{}在{}的多次增发股票名称不一致：{}".format(gpdm, dt, result))
            sorted_result = []
            for i in result:
                if i not in sorted_result:
                    sorted_result.append(i)
            return ",".join(sorted_result)
    else:       # 若无检索结果，返回Error
        return 'Error'


def update_eventlist_add_fxfs(db_p, dataset):
    conn = db_p.connection()
    cur = conn.cursor()

    for list in dataset:
        sql = "update event_list set gpmc = '{}' where gpdm = '{}' and dt = '{}' and isipo = {}".format(list[3], list[0], list[1], list[2])
        print(sql)
        cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    db_p = db_pool.get_db_pool(False)   # 初始化数据库连接池
    conn = db_p.connection()
    cur = conn.cursor()
    sql = "select gpdm, dt, isipo from event_list where gpmc is null;"  # 筛选得到股票名称为空的事件的股票代码、日期以及是否为IPO
    cur.execute(sql)
    e_list = []     # 存放待处理的数据集合
    for item in cur.fetchall():
        e_list.append([item[0], item[1], item[2]])
    # print(e_list)
    cur.close()
    conn.close()
    for i in range(0, len(e_list)):     # 遍历待处理的数据集合，获取每条记录的股票名称
        fxfs = get_gpmc(db_p, e_list[i][0], e_list[i][1], e_list[i][2])
        e_list[i].append(fxfs)
    # for item in e_list:
    #     print(item)
    update_eventlist_add_fxfs(db_p, e_list)     # 将获取到的股票名称更新进event_list表中
