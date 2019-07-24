"""
独立样本t检验
参考：
https://www.cnblogs.com/tangxianwei/p/8338092.html
"""
from scipy.stats import ttest_ind
import pandas as pd
from scipy.stats import levene
import db_pool
from xlwt import *

db = db_pool.get_db_pool(False)


def do_t_test(MODEL_TYPE, REGSET, VAR):
    print('==============================================')
    print('MODEL_TYPE = {}   REGSET = {}   VAR = {}'.format(MODEL_TYPE, REGSET, VAR))
    print('==============================================')
    conn = db.connection()
    cur = conn.cursor()
    sql = "select a.cxs_relation,b.value from event_list a " \
          "left join reg_res b on a.gpdm = b.gpdm and a.dt = b.dt " \
          "where b.model = '{}' and b.regset = '{}' " \
          "and b.var = '{}'" \
          "having b.value is not Null;".format(MODEL_TYPE, REGSET, VAR)
    cur.execute(sql)
    # result = cur.fetchall()
    result = []
    for line in cur.fetchall():
        result.append([line[0], line[1]])
    cur.close()
    conn.close()
    res_pd = pd.DataFrame(list(result), columns=['type', 'value'])
    # print(res_pd)

    X = res_pd[res_pd['type'] == 1]['value']
    Y = res_pd[res_pd['type'] == 3]['value']
    X_size = X.size
    Y_size = Y.size
    print('共分析{}条，其中相同承销商的{}条，不同承销商的{}条'.format((X_size + Y_size), X_size, Y_size))

    # print('-----检查方差齐性-----')
    # print(levene(X, Y))
    (levene_statistic, levene_pvalue) = levene(X, Y)

    if levene_pvalue > 0.05:
        print('方差齐性')
        print('-----进行独立样本t检验-----')
        print(ttest_ind(X, Y, equal_var=True))
        (ttest_statistic, ttest_pvalue) = ttest_ind(X, Y, equal_var=True)
        if ttest_pvalue > 0.05:
            print('两分类间无差距')
        else:
            print('两分类间存在差距')
        return ttest_pvalue
    else:
        print('方差不齐性')
        print('-----进行独立样本t检验-----')
        print(ttest_ind(X, Y, equal_var=False))
        (ttest_statistic, ttest_pvalue) = ttest_ind(X, Y, equal_var=True)
        if ttest_pvalue > 0.05:
            print('两分类间无差距')
        else:
            print('两分类间存在差距')
        return ttest_pvalue


def analysis_to_xls():
    book = Workbook(encoding='utf-8')
    sheet = book.add_sheet('Sheet1', cell_overwrite_ok=True)  # 创建一个sheet

    # 创建一个样式----------------------------
    style_green = XFStyle()
    pattern = Pattern()
    pattern.pattern = Pattern.SOLID_PATTERN
    pattern.pattern_fore_colour = Style.colour_map['light_green']  # 设置单元格背景色为黄色
    style_green.pattern = pattern

    style_yellow = XFStyle()
    pattern = Pattern()
    pattern.pattern = Pattern.SOLID_PATTERN
    pattern.pattern_fore_colour = Style.colour_map['yellow']  # 设置单元格背景色为黄色
    style_yellow.pattern = pattern
    # -----------------------------------------
    # sheet.write(0, 0, label='ICAO', style=style_green)  # 给第0行的第1列插入值
    # sheet.write(0, 1, label='Location')  # 给第0行的第2列插入值
    # sheet.write(0, 2, label='Airport_Name')
    # sheet.write(0, 3, label='Country')
    # book.save('air.xls')

    model_type_list = ['d1', 'd2', 's1', 's2', 'w1', 'w2']
    regset_list = ['A', 'B', 'C']
    var_list = ['prd1_sum', 'prd2_sum', 'prd3_sum','prd4_sum']
    regname_list = ['-5~-125day', '-10~-130day', '-20~-140day']
    varname_list = ['+5day', '+10day', '+20day', '+120day']
    modelname_list = ['单因子-流通市值加权', '单因子-总市值加权', '三因子-流通市值加权', '三因子-总市值加权',
                      '五因子-流通市值加权', '五因子-总市值加权']
    caution_text = 'pvalue = 0.05'

    x_index, y_index = 0, 0
    sheet.write(x_index, y_index, label=caution_text)
    x_index += 1
    for MODEL_ID, MODEL_TYPE in enumerate(model_type_list):
        sheet.write(x_index, 0, label=modelname_list[MODEL_ID], style=style_yellow)
        x_index += 1
        y_index = 0
        sheet.write(x_index, 0, label='预测集\\回归集', style=style_yellow)
        for REGSET in regname_list:
            y_index += 1
            sheet.write(x_index, y_index, label=REGSET, style=style_yellow)
        for VAR_ID, VAR in enumerate(var_list):
            x_index += 1
            sheet.write(x_index, 0, label=varname_list[VAR_ID], style=style_yellow)
            y_index = 1
            for REGSET in regset_list:
                value = do_t_test(MODEL_TYPE, REGSET, VAR)
                if value <= 0.05:
                    sheet.write(x_index, y_index, label=value, style=style_green)
                else:
                    sheet.write(x_index, y_index, label=value)
                y_index += 1
        x_index += 2

    for i in range(6):
        sheet.col(i).width = 256 * 20
    book.save('t_test_results.xls')


if __name__ == '__main__':
    # model_type_list = ['d1', 'd2', 's1', 's2', 'w1', 'w2']
    # regset_list = ['A', 'B', 'C']
    # var_list = ['prd1_sum', 'prd2_sum', 'prd3_sum','prd4_sum']
    #
    # for MODEL_TYPE in model_type_list:
    #     for REGSET in regset_list:
    #         for VAR in var_list:
    #             do_t_test(MODEL_TYPE, REGSET, VAR)

    analysis_to_xls()



