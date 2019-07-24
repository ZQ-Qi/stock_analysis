"""
统计所有事件中名字更改的情况
这个程序没啥用了，wind中的股票名称是全局改的，同一个股票名字都一样，日了狗了
"""
import db_pool


def sort_list(mc_list):
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
    对两个股票名称进行比对，如果存在2个及以上的相同字符，则认为是正常的增发事件，否则认为是借壳上市
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
    if count_char > 1:
        return True
    else:
        return False

    # return mc1 == mc2


def analysis_gp(db_p, gpdm):
    # print(type(db_p))
    conn = db_p.connection()
    cur = conn.cursor()
    sql = "select dt from event_list where gpdm = '{}' order by seq;".format(gpdm)
    cur.execute(sql)
    dt_list = []
    for item in cur.fetchall():
        dt_list.append(item[0])

    gpmc_list = []
    for dt in dt_list:
        sql = "select gpmc from history_price where gpdm = '{}' and dt >= '{}' order by dt ASC limit 1;".format(gpdm, dt)
        cur.execute(sql)
        gpmc_list.append(cur.fetchone()[0])
    cur.close()
    conn.close()

    count = 0
    sum = len(gpmc_list) - 1
    for i in range(1, len(gpmc_list)):
        if not is_same_company(gpmc_list[i-1], gpmc_list[i]):
            count += 1
    rate = count / sum
    print("股票{}在{}次对比中更改名称{}次，占比{:.2f}，详情：{}".format(gpdm, sum, count, rate, gpmc_list))
    return [count, sum, rate]


if __name__ == '__main__':
    db_p = db_pool.get_db_pool(False)
    conn = db_p.connection()
    cur = conn.cursor()
    sql = "select distinct gpdm from event_list where e_sum > 1;"
    cur.execute(sql)
    gp_list = []
    for dm in cur.fetchall():
        gp_list.append(dm[0])
    cur.close()
    conn.close()
    change_count = 0
    summary = 0
    for gp in gp_list:
        res = analysis_gp(db_p, gp)
        change_count += res[0]
        summary += res[1]
    print("--------总结--------")
    print("增发总数：{}".format(summary))
    print("更名总数：{}".format(change_count))
    print("占比：{:.2f}".format((change_count/summary)))

