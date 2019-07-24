"""
对用于回归的数据进行检查
"""

import timeline
import db_pool
import thread_pool
import time


def get_history_set(db_p, gpdm, start_dt, end_dt, include_early = False):
    conn = db_p.connection()
    cur = conn.cursor()
    if include_early:
        sql = "select dt,syl,dssyl from history_price where gpdm = '{}' and dt < '{}' and dt >= '{}' order by dt ASC;".format(gpdm, end_dt, start_dt)
    else:
        sql = "select dt,syl,dssyl from history_price where gpdm = '{}' and dt <= '{}' and dt > '{}' order by dt ASC;".format(gpdm, end_dt, start_dt)
    cur.execute(sql)
    dataset = []
    for item in cur.fetchall():
        dataset.append([item[0], item[1], item[2]])
    cur.close()
    conn.close()
    return dataset


def get_history_count(db_p, gpdm, start_dt, end_dt, include_early = False):
    conn = db_p.connection()
    cur = conn.cursor()
    if include_early:
        sql = "select count(*) from history_price where gpdm = '{}' and dt < '{}' and dt >= '{}' order by dt ASC;".format(
            gpdm, end_dt, start_dt)
    else:
        sql = "select count(*) from history_price where gpdm = '{}' and dt <= '{}' and dt > '{}' order by dt ASC;".format(
            gpdm, end_dt, start_dt)
    cur.execute(sql)
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count


def get_history_check(thread_name, db_p, gpdm, e_dt):
    print("{} is analysing {} in {}".format(thread_name, gpdm, e_dt))
    e_dtid_before = timeline.get_dtid_before_by_dt(db_p, e_dt)  # 事前时间戳
    e_dtid_after = timeline.get_dtid_after_by_dt(db_p, e_dt)    # 事后时间戳

    dt_5 = timeline.get_dt_by_dtid(db_p, e_dtid_before - 5)
    dt_125 = timeline.get_dt_by_dtid(db_p, e_dtid_before - 125)
    if dt_125 is not None and dt_5 is not None:
        sum_5 = get_history_count(db_p, gpdm, dt_125, dt_5)
    else:
        sum_5 = -1

    dt_10 = timeline.get_dt_by_dtid(db_p, e_dtid_before - 10)
    dt_130 = timeline.get_dt_by_dtid(db_p, e_dtid_before - 130)
    if dt_10 is not None and dt_130 is not None:
        sum_10 = get_history_count(db_p, gpdm, dt_130, dt_10)
    else:
        sum_10 = -1

    dt_20 = timeline.get_dt_by_dtid(db_p, e_dtid_before - 20)
    dt_140 = timeline.get_dt_by_dtid(db_p, e_dtid_before - 140)
    if dt_20 is not None and dt_140 is not None:
        sum_20 = get_history_count(db_p, gpdm, dt_140, dt_20)
    else:
        sum_20 = -1

    dt2_0 = timeline.get_dt_by_dtid(db_p, e_dtid_after)
    dt2_10 = timeline.get_dt_by_dtid(db_p, e_dtid_after + 10)
    if dt2_10 is not None:
        sum2_10 = get_history_count(db_p, gpdm, dt2_0, dt2_10)
    else:
        sum2_10 = -1

    dt2_20 = timeline.get_dt_by_dtid(db_p, e_dtid_after + 20)
    if dt2_20 is not None:
        sum2_20 = get_history_count(db_p, gpdm, dt2_0, dt2_20)
    else:
        sum2_20 = -1

    dt2_40 = timeline.get_dt_by_dtid(db_p, e_dtid_after + 40)
    if dt2_40 is not None:
        sum2_40 = get_history_count(db_p, gpdm, dt2_0, dt2_40)
    else:
        sum2_40 = -1

    dt2_240 = timeline.get_dt_by_dtid(db_p, e_dtid_after + 240)
    if dt2_240 is not None:
        sum2_240 = get_history_count(db_p, gpdm, dt2_0, dt2_240)
    else:
        sum2_240 = -1

    conn = db_p.connection()
    cur = conn.cursor()

    # 判断是否存在日期超出时间限制，若是则将其存为Null
    if dt_140 is not None and dt2_240 is not None:
        sql = "insert into check_dataset values ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');"\
            .format(gpdm, e_dt, sum_5, sum_10, sum_20, sum2_10, sum2_20, sum2_40, sum2_240, dt_140, dt2_240)
    elif dt_140 is None and dt2_240 is not None:
        sql = "insert into check_dataset values ('{}','{}','{}','{}','{}','{}','{}','{}','{}',Null,'{}');" \
            .format(gpdm, e_dt, sum_5, sum_10, sum_20, sum2_10, sum2_20, sum2_40, sum2_240, dt2_240)
    elif dt_140 is not None and dt2_240 is None:
        sql = "insert into check_dataset values ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}', Null);" \
            .format(gpdm, e_dt, sum_5, sum_10, sum_20, sum2_10, sum2_20, sum2_40, sum2_240, dt_140)
    else:
        sql = "insert into check_dataset values ('{}','{}','{}','{}','{}','{}','{}','{}','{}',Null,Null);" \
            .format(gpdm, e_dt, sum_5, sum_10, sum_20, sum2_10, sum2_20, sum2_40, sum2_240)

    try:
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
        return 'SUCCESS'
    except Exception as e:
        cur.close()
        conn.close()
        return [e, sql]


def get_elist_to_be_done(db_p):
    """
    获取等待回归运算的事件列表
    :param db_p:
    :return:['股票代码', '事件日期']
    """
    conn = db_p.connection()
    cur = conn.cursor()
    sql = "select gpdm,dt from event_list where seq != 1 and ischeck = 0;"
    cur.execute(sql)
    e_list = []
    for item in cur.fetchall():
        e_list.append([item[0], item[1]])
    cur.close()
    conn.close()
    return e_list


def callback(status, result):
    print(status, result)
    pass


def run():
    db_p = db_pool.get_db_pool(True)
    pool = thread_pool.ThreadPool(30)

    e_list = get_elist_to_be_done(db_p)
    for item in e_list:
        pool.put(get_history_check, (db_p, item[0], item[1], ), callback)
        # get_history_line(db_p, item[0], item[1])

    while True:
        time.sleep(2)
        if len(pool.generate_list) - len(pool.free_list) == 0:
            print("Task finished! Closing...")
            pool.close()
            break
        else:
            print("{} Threads still working.Wait.".format(len(pool.generate_list) - len(pool.free_list)))
            pass

if __name__ == '__main__':
    run()
