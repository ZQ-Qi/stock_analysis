"""
由于原始资料中证监会行业存在大量缺失，在此情况下，筛选无证监会行业的发行事件，并获取该股所有发行事件，若存在某一事件有证监会行业记录，则对其余事件的证监会行业进行补充。

2019-09-02
"""
import db_pool

# 初始化数据库连接
db = db_pool.get_db_pool(False)
conn = db.connection()
cursor = conn.cursor()


def get_gpdm_list():
    """
    获取存在证监会行业缺失的发行事件的股票代码
    :return:
    """
    gpdm_list = []
    sql = "select distinct gpdm from event_list where zjhhy is Null;"  # 获取存在证监会行业缺失情况的股票代码（无重复）
    cursor.execute(sql)
    for i in cursor.fetchall():
        gpdm_list.append(i[0])
    return gpdm_list


def complete_zjhhy_by_same_gpdm(gpdm):
    """
    判断该股票是否存在有证监会行业的发行事件：
    若存在多个，判断是否一致，一致则对该股证监会行业缺失事件的证监会行业进行补充
    若存在一个，则以此作为该股其他发行事件的证监会行业
    若不存在，则不进行处理，留待后续
    :return:
    """
    sql = "select zjhhy from event_list where zjhhy is not Null and gpdm = '{}'".format(gpdm) # 查询该股是否存在有证监会行业的事件
    hy_exist_count = cursor.execute(sql)

    # 若存在一条记录，则以该记录的证监会行业为该股其他发行时间补充证监会行业
    if hy_exist_count == 1:
        zjhhy = cursor.fetchone()[0] + '*'      # 对补充的证监会行业以*结尾作为标记
        sql = "update event_list set zjhhy = '{}' where zjhhy is Null and gpdm = '{}'".format(zjhhy, gpdm)
        cursor.execute(sql)
    elif hy_exist_count > 1:
        zjhhy_list = []
        for i in cursor.fetchall():
            zjhhy_list.append(i[0])
        zjhhy = zjhhy_list[0]
        for i in zjhhy_list:
            if zjhhy != i:      # 如果存在多个证监会行业且互相不一致，则不进行处理
                print("股票{}存在多个不同的证监会行业记录，跳过")
                return 0
        zjhhy = zjhhy + '*'
        sql = "update event_list set zjhhy = '{}' where zjhhy is Null and gpdm = '{}'".format(zjhhy, gpdm)
        cursor.execute(sql)
    else:
        pass        # 若该股票所有发行事件均未记载证监会行业，则不进行处理
    conn.commit()


if __name__ == '__main__':
    gpdm_list = get_gpdm_list()
    for gpdm in gpdm_list:
        complete_zjhhy_by_same_gpdm(gpdm)




cursor.close()
conn.close()
