"""
用于根据时间获取开盘日序号或者根据开盘日序号获取日期
"""
import db_pool


def get_dtid_before_by_dt(db, dt, include=False):
    """
    通过日期获取该日期前最近的开盘日序号
    如果该日期也是开盘日，默认取前一个开盘日
    :param db:
    :param dt:
    :param include: 如果当日为交易日，是否包含，默认不包含去上一个交易日
    :return:
    """
    conn = db.connection()
    cur = conn.cursor()
    if include:
        sql = "select dtid from szzz where dt <= '{}' order by dt DESC limit 1;".format(dt)
    else:
        sql = "select dtid from szzz where dt < '{}' order by dt DESC limit 1;".format(dt)
    cur.execute(sql)
    dtid = cur.fetchone()[0]
    cur.close()
    conn.close()
    return dtid


def get_dtid_after_by_dt(db, dt, include=False):
    """
    通过日期获取该日期后最近的开盘日序号
    如果该日期也是开盘日，默认取下一个开盘日
    :param db:
    :param dt:
    :param include:
    :return:
    """
    conn = db.connection()
    cur = conn.cursor()
    if include:
        sql = "select dtid from szzz where dt >= '{}' order by dt ASC limit 1;".format(dt)
    else:
        sql = "select dtid from szzz where dt > '{}' order by dt ASC limit 1;".format(dt)
    cur.execute(sql)
    dtid = cur.fetchone()
    if dtid is not None:    # 保证能够获取到dtid，获取不到则返回None
        dtid = dtid[0]
    cur.close()
    conn.close()
    return dtid


def get_dt_by_dtid(db, dtid):
    """
    通过dtid查找对应日期，如果日期不存在则返回None
    :param db:
    :param dtid:
    :return:
    """
    conn = db.connection()
    cur = conn.cursor()
    sql = "select dt from szzz where dtid = '{}'".format(dtid)
    cur.execute(sql)
    dt = cur.fetchone()
    cur.close()
    conn.close()
    if dt is None:
        return None
    else:
        return dt[0]


if __name__ == '__main__':
    db = db_pool.get_db_pool(False)
    dt = '2007-12-28'  # 5177
    # dt = '2019-04-10'
    print(get_dtid_before_by_dt(db, dt, False))
    print(get_dtid_before_by_dt(db, dt, True))
    print(get_dtid_after_by_dt(db, dt, False))
    print(get_dtid_after_by_dt(db, dt, True))
    print(get_dt_by_dtid(db, 0))

