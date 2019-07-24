import db_pool
import datetime

# 获取更名表存为元组namechange_list
namechange_file = 'cxs_namechange_list.csv'
namechange_list = []
with open(namechange_file, 'r', encoding='gbk') as f:
    f_context = f.read()
    f_lines = f_context.split('\n')
    f_lines = f_lines[:-1]
    for f_line in f_lines:
        line = f_line.split(',')
        sorted_line = [i for i in line if i != '']
        if len(sorted_line) >= 1:
            namechange_list.append(tuple(sorted_line))
namechange_list = tuple(namechange_list)

# for i, j in enumerate(namechange_list):
#     print('{} - {}'.format(i,j))

weak_relation_file = 'weak_relation.csv'
weak_relation_list = []
with open(weak_relation_file, 'r', encoding='gbk') as f:
    f_context = f.read()
    f_lines = f_context.split('\n')
    f_lines = f_lines[:-1]
    for f_line in f_lines:
        line = f_line.split(",")
        if len(line) >= 1:
            weak_relation_list.append(tuple(line))
weak_relation_list = tuple(weak_relation_list)

# 创建数据库连接
db = db_pool.get_db_pool(False)


def double_index(dd_tuple, search_str):
    """
    分析字符串在二维元组中的位置，返回第一维和第二维坐标，若查找不到则返回[-1，-1]
    :param dd_tuple:
    :param search_str:
    :return:
    """
    for i, i_tuple in enumerate(dd_tuple):
        if search_str in i_tuple:
            j = i_tuple.index(search_str)
            return [i, j]
    return [-1, -1]


weak_relation_index_list = []
for [date, cxs1, cxs2] in weak_relation_list:
    [cxs_index1, _] = double_index(namechange_list, cxs1)
    [cxs_index2, _] = double_index(namechange_list, cxs2)
    weak_relation_index_list.append(tuple([date, cxs_index1, cxs_index2]))
# for i in range(8):
#     print('{} \n{}\n'.format(weak_relation_list[i], weak_relation_index_list[i]))


def split_cxs_cwgw(zcxs, cwgw):
    """
    将主承销商和财务顾问按照逗号分隔为字符串列表，便于后续分析
    :param zcxs:
    :param cwgw:
    :return:
    """
    res_set = []
    if zcxs is not None:
        res_set += zcxs.split(',')
    if cwgw is not None:
        res_set += cwgw.split(',')
    res_set = list(set(res_set))
    return res_set


def get_event_list():
    """
    获取待处理的股票列表
    :return: [gpdm, dt, seq, relation, cxs_list]
    """
    event_list = []
    conn = db.connection()
    cur = conn.cursor()
    sql = "select gpdm, dt, seq, cxs_relation, zcxs, cwgw from event_list where cxs_relation = 3;"
    cur.execute(sql)
    for i in cur.fetchall():
        event_list.append([i[0], i[1], i[2], i[3], split_cxs_cwgw(i[4], i[5])])
    cur.close()
    conn.close()
    return event_list


def update_cxs_relation(result_set):
    """
    将分析结果更新至数据库
    :param result_set: [gpdm, dt, seq, relation, cxs_list]
    :return:
    """
    conn = db.connection()
    cur = conn.cursor()
    relation_change_count = 0
    for i in result_set:
        if i[3] == 3:
            continue
        sql = "update event_list set cxs_relation = {} where gpdm = '{}' and dt = '{}'".format(i[3], i[0], i[1])
        cur.execute(sql)
        relation_change_count += 1
    # conn.commit()
    print("======== REPORT ========")
    print("共更新{}条记录".format(relation_change_count))
    cur.close()
    conn.close()


def get_cxslist_by_seq(gpdm, seq):
    """
    根据股票代码和发行次序获取该股票该次序的主承销商和财务顾问
    :param gpdm:
    :param seq:
    :return:
    """
    conn = db.connection()
    cur = conn.cursor()
    sql = "select zcxs, cwgw from event_list where gpdm = '{}' and seq = {}".format(gpdm, seq)
    cur.execute(sql)
    query_res = cur.fetchone()
    cur.close()
    conn.close()
    return split_cxs_cwgw(query_res[0], query_res[1])


def weak_link_check(cxs1, cxs2, event_date):
    [i, _] = double_index(namechange_list, cxs1)    # 获取cxs1在namechangelist中的行位置
    [j, _] = double_index(namechange_list, cxs2)    # 获取cxs2在namechangelist中的行位置
    for line_num, line in enumerate(weak_relation_index_list):
        if i in line:
            if j in line:
                date_str = weak_relation_index_list[line_num][0]
                if date_str == '':
                    print('======weak relation======')
                    print('{} - {} - {} - {}'.format(cxs1, cxs2, date, weak_relation_index_list[line_num]))
                    return True
                else:
                    relation_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                    if relation_date <= event_date:
                        print('======weak relation======')
                        print('{} - {} - {} - {}'.format(cxs1, cxs2, date, weak_relation_index_list[line_num]))
                        return True
    return False


def analysis_relation(cxs_a, cxs_b):
    res = 3
    for a in cxs_a:
        for b in cxs_b:     # 对a组和b组的承销商进行两两比对
            if a == b:      # 如果完全符合，则认为是同一家
                res = 1
            else:           # 如果完全匹配失败，则进行深度匹配
                [i, _] = double_index(namechange_list, a)
                if b in namechange_list[i]:
                    res = 1

    return res


def run():
    event_list = get_event_list()       # 获取待处理的股票列表[gpdm, dt, seq, relation, cxs_list]
    for item in event_list:             # 遍历待处理的股票列表
        cxs_a = item[4]                 # cxs_a为本次事件承销商列表
        cxs_b = get_cxslist_by_seq(item[0], (item[2]-1))        # cxs_b 获取上次事件承销商列表
        res = analysis_relation(cxs_a, cxs_b)       # 计算两组承销商是否存在联系，1-强相关；2-弱相关
        if res == 1:        # 如果计算值为强相关，输出提醒，并在列表中修改relation列的值
            item[3] = res
            print("【1】{}-{}-{}".format(item[0], item[1], item[2]))

        if weak_link_check(cxs_a, cxs_b, item[1]):
            item[3] = 2
            print("【2】{}-{}-{}".format(item[0], item[1], item[2]))

    update_cxs_relation(event_list)     # 更新数据库，应用更改


if __name__ == '__main__':
    pass
    run()
