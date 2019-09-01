'''
从新浪财经获取股票每日股价信息
'''

import urllib.request
import pymysql
import settings
import os


def get_csv(stockid):
    """
    根据股票代码下载该股的所有历史股价数据，并在dl_datapath中存储为以股票代码为名的csv文件
    :param stockid: 股票代码
    :return:
    """
    if stockid[0:1] == "6":
        stock_code = "0" + stockid
    else:
        stock_code = "1" + stockid
    print("<get_csv> is trying to download data {}".format(stock_code))
    url = "http://quotes.money.163.com/service/chddata.html?code={}&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP".format(stock_code)
    print(url)
    file_path = "dl_datapath/{}.csv".format(stockid)
    urllib.request.urlretrieve(url, file_path)


def get_filename_by_path():
    """
    获取dl_datapath下的文件列表
    :return:
    """
    path = "dl_datapath"
    files_list = os.listdir(path)
    filename_list = []
    for item in files_list:
        filename_list.append(item[:-4])
    return filename_list

def download_by_db():
    conn = pymysql.connect(host=settings.host, port=settings.port, user=settings.user,
                           password=settings.password,db=settings.db, charset='utf8')   # 创建数据库连接
    cur = conn.cursor()
    sql = "select distinct gpdm from zfss;"  # 从增发实施表中获取股票代码列表
    stock_list = []
    try:
        cur.execute(sql)
        results = cur.fetchall()
        for row in results:
            stock_list.append(row[0])
    except pymysql.Error as e:
        print("ERROR: {}".format(e))

    finished_list = get_filename_by_path()  # 获取已下载完成的股票代码
    task_list = [x for x in stock_list if x not in finished_list]   # 总股票代码列表减去已下载完成的股票代码列表，获得本次要爬取的股票代码列表

    cur.close()
    conn.close()
    # 遍历需爬取历史信息的股票代码列表，并开始爬取
    for item in task_list:
        get_csv(item)

if __name__ == '__main__':
    download_by_db()

