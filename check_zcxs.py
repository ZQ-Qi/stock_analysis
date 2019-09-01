"""
检查主承销商或财务顾问与上次发行是否一致，并在event_list进行标识
"""
import db_pool


def deep_analysis_model(q_zcxs, q_cwgw, zcxs, cwgw):
    pass


def sort_list(mc_list):
    """
    去除字符列表中的空格、S、T、*、N，避免干扰
    :param mc_list:
    :return:
    """
    if ' ' in mc_list:
        mc_list.remove(' ')
    if 'S' in mc_list:
        mc_list.remove('S')
    if 'T' in mc_list:
        mc_list.remove('T')
    if '*' in mc_list:
        mc_list.remove('*')
    if 'N' in mc_list:
        mc_list.remove('N')

    return mc_list


def is_same_company(mc1, mc2):
    """
    对两个股票名称进行比对，如果存在2个及以上的相同字符，则认为是正常的增发事件(0)，否则认为是借壳上市(1)
    比对时去除*STN及空格等标识避免干扰
    :param mc1:
    :param mc2:
    :return:
    """
    mc1_list = sort_list(list(mc1))
    mc2_list = sort_list(list(mc2))

    count_char = 0
    for i in mc1_list:
        if i in mc2_list:
            count_char += 1
    if count_char > 1:      # 正常事件
        return 0
    else:                   # 借壳上市
        return 1


def is_name_change(db, gpdm, q_dt, dt):
    print('{} - {} - {}'.format(gpdm,q_dt,dt))
    conn = db.connection()
    cur = conn.cursor()
    sql = "select gpmc from history_price where gpdm = '{}' and dt >= '{}' order by dt ASC limit 1;".format(gpdm, dt)
    cur.execute(sql)
    mc = cur.fetchone()[0]
    sql = "select gpmc from history_price where gpdm = '{}' and dt >= '{}' order by dt ASC limit 1;".format(gpdm, q_dt)
    cur.execute(sql)
    q_mc = cur.fetchone()[0]
    cur.close()
    conn.close()
    return is_same_company(mc, q_mc)


def is_same_zcxs(q_zcxs, q_cwgw, zcxs, cwgw):
    q_jigou = []
    jigou = []
    if q_zcxs is not None:
        q_jigou += q_zcxs.split(',')
    if q_cwgw is not None:
        q_jigou += q_cwgw.split(',')
    if zcxs is not None:
        jigou += zcxs.split(',')
    if cwgw is not None:
        jigou += cwgw.split(',')
    # 去重
    q_jigou = list(set(q_jigou))
    jigou = list(set(jigou))

    sum_same = 0
    for i in q_jigou:
        for j in jigou:
            if i == j:
                sum_same += 1
    return sum_same


def check_zcxs(db, gpdm):
    print('processing gp: {}'.format(gpdm))
    conn = db.connection()
    cur = conn.cursor()
    sql = "select gpdm, dt, zcxs, cwgw, seq, e_sum from event_list where gpdm = '{}' order by seq;".format(gpdm)
    event_list = []
    cur.execute(sql)
    for line in cur.fetchall():
        event_list.append([line[0], line[1], line[2], line[3], line[4], line[5]])
    event_list[0].append(-1)
    event_list[0].append(0)

    for i in range(1, len(event_list)):
        event_list[i].append(is_same_zcxs(event_list[i-1][2], event_list[i-1][3], event_list[i][2], event_list[i][3]))
        event_list[i].append(is_name_change(db, event_list[i][0], event_list[i-1][1], event_list[i][1]))

    for e in event_list:
        print(e)
        sql = "update event_list set is_same_cxs = {}, is_name_change = {} where gpdm = '{}' and dt = '{}';".format(e[6],e[7],e[0],e[1])
        cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()


def get_gpdm_list(db):
    conn = db.connection()
    cur = conn.cursor()
    sql = "select distinct gpdm from event_list;"
    cur.execute(sql)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result


def run():
    db = db_pool.get_db_pool(False)
    gp_tumple = get_gpdm_list(db)
    for i in range(len(gp_tumple)):
        gpdm = gp_tumple[i][0]
        check_zcxs(db,gpdm)



if __name__ == '__main__':
    run()


