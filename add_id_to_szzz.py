"""
为上证综指列表添加编号，作为股市开盘日的时间序列，以便推算股市开盘日的天数
"""
import db_pool

db_p = db_pool.get_db_pool(False)

conn = db_p.connection()
cur = conn.cursor()
sql = "select dt from szzz order by dt ASC;"
cur.execute(sql)
dt_list = []
i = 1000
for dt in cur.fetchall():
    dt_list.append([dt[0], i])
    i += 1
for item in dt_list:
    sql = "update szzz set dtid = {} where dt = '{}'".format(item[1], item[0])
    print(sql)
    cur.execute(sql)
conn.commit()
cur.close()
conn.close()
