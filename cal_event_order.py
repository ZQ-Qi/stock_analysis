"""
对event_list表进行分析，得到每个个股的事件序号
"""
import db_pool

def cal_stock_order(db_p, gpdm):
    conn = db_p.connection()
    cur = conn.cursor()
    sql = "select gpdm,dt from event_list where gpdm = '{}' order by dt ASC;".format(gpdm)
    cur.execute(sql)
    e_list = []
    for item in cur.fetchall():
        e_list.append([item[0],item[1]])
    # print(e_list)
    # print(len(e_list))
    e_sum = len(e_list)
    # 对矩阵进行运算，添加次序seq，前序时差day_before，后序时差day_after，个股事件总数e_sum
    for i in range(0, e_sum):
        seq = i + 1
        if e_sum == 1:
            day_before = 'Null'
            day_after = 'Null'
        elif i == 0:
            day_before = 'Null'
            day_after = (e_list[i+1][1] - e_list[i][1]).days

        elif i == e_sum-1:
            day_before = (e_list[i][1] - e_list[i-1][1]).days
            day_after = 'Null'
        else:
            day_before = (e_list[i][1] - e_list[i-1][1]).days
            day_after = (e_list[i+1][1] - e_list[i][1]).days
        e_list[i].append(seq)
        e_list[i].append(day_before)
        e_list[i].append(day_after)
        e_list[i].append(e_sum)
    for item in e_list:
        sql = "update event_list set seq = {}, day_before = {}, day_after = {}, e_sum = {} " \
              "where gpdm = '{}' and dt = '{}';".format(item[2], item[3], item[4], item[5], item[0], item[1])
        print(sql)
        cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    db_p = db_pool.get_db_pool(False)
    conn = db_p.connection()
    cur = conn.cursor()
    sql = "select distinct gpdm from event_list;"
    cur.execute(sql)
    gp_list = []
    for gp in cur.fetchall():
        gp_list.append(gp[0])
    cur.close()
    conn.close()
    for dm in gp_list:
        cal_stock_order(db_p, dm)
    # print(gp_list)