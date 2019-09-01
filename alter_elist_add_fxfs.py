"""
为event_list添加fxfs列
"""
import db_pool

def get_fxfs_from_zfss(db_p, gpdm, dt):
    conn = db_p.connection()
    cur = conn.cursor()
    sql = "select fxfs from zfss where gpdm = '{}' and ssggr = '{}';".format(gpdm, dt)
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
            print("{}在{}的增发存在多种发行方式：{}".format(gpdm, dt, result))
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
        sql = "update event_list set fxfs = '{}' where gpdm = '{}' and dt = '{}'".format(list[2], list[0], list[1])
        cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    db_p = db_pool.get_db_pool(False)
    conn = db_p.connection()
    cur = conn.cursor()
    sql = "select gpdm, dt from event_list where isipo = 0 and fxfs is null;"
    cur.execute(sql)
    e_list = []
    for item in cur.fetchall():
        e_list.append([item[0], item[1]])
    # print(e_list)
    cur.close()
    conn.close()
    for i in range(0, len(e_list)):
        fxfs = get_fxfs_from_zfss(db_p, e_list[i][0], e_list[i][1])
        e_list[i].append(fxfs)
    update_eventlist_add_fxfs(db_p, e_list)
