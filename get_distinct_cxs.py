"""
获取事件列表中<<<2009年>>>之后的主承销商和财务顾问，对多个财务顾问或主承销商的进行分割，并筛选去重得到承销商和财务顾问的列表
"""
import db_pool

# 初始化数据库连接
db = db_pool.get_db_pool(False)
conn = db.connection()
cur = conn.cursor()

sql = "select distinct zcxs from event_list where dt > '2009-01-01' union select distinct cwgw from event_list where dt > '2009-01-01';"
cur.execute(sql)
unique_res = []
for line in cur.fetchall():
    if line[0] is None:
        continue
    res_list = line[0].split(',')
    for i in res_list:
        i = i.strip()
        if i not in unique_res:
            unique_res.append(i)
cur.close()
conn.close()
for i in unique_res:
    print(i)
