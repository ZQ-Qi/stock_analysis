"""
导入wind和csmar的原始数据
"""
import csv
import pymysql
import settings

def csv_reader(filename):
    # 读取文件，得到数据表阵列
    with open(filename, 'r', encoding='gbk') as f:
        data = csv.reader(f)
        dataset = []
        for line in data:
            dataset.append(line)
        f.close()
        return dataset

def comma_str(line, flag):  # flag==1 元素不加引号; flag==2 元素加引号
    str = '('
    if flag == 0:
        for item in line:
            str = str + item + ', '
        return str[:-2] + ")"
    else:
        for item in line:
            str = str + "\"" + item + "\", "
        return str[:-2] + ")"


def import_data_to_mysql(filename, sheet):
    # 建立数据库连接
    conn = pymysql.connect(host=settings.host, port=settings.port, user=settings.user,
                           password=settings.password, db=settings.db, charset='utf8')
    cur = conn.cursor()

    # 开始整理数据
    dataset = csv_reader(filename)  # 读取csv
    header = dataset[0]  # 获取数据矩阵的表头
    del (dataset[0])  # 去除数据矩阵中的表头
    total_sum = len(dataset)
    err_sum = 0
    for line in dataset:  # 对每一行进行分析
        tHeader = []  # 每一行的header
        tLine = []  # 每一行的元素
        i = 0  # 初始化计数器
        for item in line:  # 遍历每一行元素，存在空值即不插入该列
            if item.strip():
                tHeader.append(header[i].lower())
                tLine.append(line[i])
            i = i + 1
        sql = "insert into " + sheet + comma_str(tHeader, 0) + "values" + comma_str(tLine, 1) + ";"

        try:
            cur.execute(sql)

        except pymysql.Error as e:
            err_sum += 1
            print('ERROR： {}'.format(e))
            print(sql)
    print('----summary------')
    print('total: {}    success: {}    err: {}'.format(total_sum, (total_sum-err_sum), err_sum))
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    # 设置要导入的工作表
    #
    # ----1. 上市公司基本情况文件163414409----- #
    # filename = 'origin_data/RS_Cobasic.csv'
    # sheet = 'or_cobasic'

    # ----2. 上市公司增、配承销商文件163515200----- #
    # filename = 'origin_data/RS_Csne.csv'
    # sheet = 'or_csne'

    # ----3. 上市公司增、配上市时十大股东持股情况文件163538018----- #
    # filename = 'origin_data/RS_Ttshs.csv'
    # sheet = 'or_ttshs'

    # ----4. 上市公司增、配管理层人员文件163614785----- #
    # filename = 'origin_data/RS_Exec.csv'
    # sheet = 'or_exec'

    # ----5. 上市公司增、配前财务文件163639176----- #
    # filename = 'origin_data/RS_Finan.csv'
    # sheet = 'or_finan'

    # ----6. 上市公司增、配当年财务盈利预测文件163714568----- #
    # filename = 'origin_data/RS_Profcast.csv'
    # sheet = 'or_profcast'

    # ----7. 上市公司转配股基本情况文件163747216----- #
    # filename = 'origin_data/RS_Trbasic.csv'
    # sheet = 'or_trbasic'

    # ----8. 上市公司增发发行基本情况文件163814132----- #
    # filename = 'origin_data/RS_Aibasic.csv'
    # sheet = 'or_aibasic'

    # ----9. 上市公司配股基本情况文件163833570----- #
    # filename = 'origin_data/RS_Robasic.csv'
    # sheet = 'or_robasic'

    # ----10. 上市公司增、配前后日收益率变化文件163855192----- #
    # filename = 'origin_data/RS_Robacdler.csv'
    # sheet = 'or_robacdler'

    # ----11. 上市公司增、配前后股本变动情况文件163915217----- #
    # filename = 'origin_data/RS_Capc.csv'
    # sheet = 'or_capc'

    # ----上证综指----- #
    # filename = 'origin_data/szzz.csv'
    # sheet = 'szzz'

    # ----沪深300----- #
    # filename = 'origin_data/沪深300.csv'
    # sheet = 'hssb'

    # ----新股发行资料----- #
    # filename = 'origin_data/xgfxzl.csv'
    # sheet = 'xgfxzl'

    # ----增发实施----- #
    # filename = 'origin_data/zfss.csv'
    # sheet = 'zfss'

    # ----三因子模型（日）----- #
    filename = 'origin_data/syzmx.csv'
    sheet = 'syzmx'

    # ----五因子模型（日）----- #
    # filename = 'origin_data/wyzmx.csv'
    # sheet = 'wyzmx'

    import_data_to_mysql(filename, sheet)




