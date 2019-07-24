"""
计算股票历史交易数据的收益率和对数收益率
"""
import db_pool
import thread_pool
import math
import logging
import time

def thread_work(thread_name, db_p, gpdm, dt, spj, qsp):
    print("start processing {} - {} - {}".format(thread_name, gpdm, dt))
    conn = db_p.connection()
    cur = conn.cursor()
    try:
        spj = float(spj)
        qsp = float(qsp)
        sql = "update history_price set syl = '{}', dssyl = '{}' where gpdm = '{}' and dt = '{}'".format(syl, dssyl, gpdm, dt)
        # print(sql)
        cur.execute(sql)
        conn.commit()

    except Exception as err:
        print("ERR({} - {}): {}".format(gpdm, dt, err))
        logging.error("ERR({} - {}): {}".format(gpdm, dt, err))
        pass
    finally:
        cur.close()
        conn.close()

def callback(status, result):
    """
    根据需要进行的回调函数，默认不执行。
    :param status: action函数的执行状态
    :param result: action函数的返回值
    :return:
    """
    # print('thread finish status: {}'.format(status))
    # print('thread finish result: {}'.format(result))
    pass



if __name__ == '__main__':
    # 初始化Logging
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',
                        level=logging.DEBUG, filemode='a',
                        filename='cal_history_syl.log')
    # 初始化数据库连接池
    logging.info('initialize the db pool.')
    db_p = db_pool.get_db_pool(True)

    # 创建线程池
    logging.info('create thread pool.')
    print('create thread pool.')
    pool = thread_pool.ThreadPool(30)

    logging.info('get the list to be calculate.')
    conn = db_p.connection()
    cur = conn.cursor()
    sql = "select gpdm, dt, spj, qsp from history_price where syl is null;"
    cur.execute(sql)
    while True:
        r = cur.fetchone()
        if r is not None:
            pool.put(thread_work, (db_p, r[0], r[1], r[2], r[3],), callback)
        else:
            break

    cur.close()
    conn.close()

    while True:
        time.sleep(1)
        if len(pool.generate_list) - len(pool.free_list) == 0:
            print("Task finished! Closing...")
            pool.close()
            break
        else:
            print("{} Threads still working.Wait.".format(len(pool.generate_list) - len(pool.free_list)))
            pass


