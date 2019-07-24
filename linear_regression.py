"""
执行回归进程
"""
import db_pool
import thread_pool
import timeline
import time

import pandas as pd
from statsmodels.formula.api import ols

dataset_header = [['GPDM', 'DT', 'SYL', 'DSSYL', 'WFXLL',
                   'S_RP1', 'S_SMB1', 'S_HML1',
                   'S_RP2', 'S_SMB2', 'S_HML2',
                   'W_RP1', 'W_SMB1', 'W_HML1', 'W_RMW1', 'W_CMA1',
                   'W_RP2', 'W_SMB2', 'W_HML2', 'W_RMW2', 'W_CMA2']]


def get_variables_of_model(model, res_dict):
    res_dict['rsquared'] = model.rsquared
    res_dict['rsquared_adj'] = model.rsquared_adj
    res_dict['fvalue'] = model.fvalue
    res_dict['f_pvalue'] = model.f_pvalue
    res_dict['llf'] = model.llf
    res_dict['ssr'] = model.ssr
    res_dict['nobs'] = model.nobs
    res_dict['mse_model'] = model.mse_model
    res_dict['mse_resid'] = model.mse_resid
    res_dict['mse_total'] = model.mse_total
    return res_dict


def get_variables_of_predict(model, res_dict, dataf_test1=None, dataf_test2=None, dataf_test3=None, dataf_test4=None):
    if dataf_test1 is not None:
        dataf_test1['FXSYL_prd'] = model.predict(dataf_test1)
        dataf_test1['diff'] = dataf_test1['FXSYL'] - dataf_test1['FXSYL_prd']
        res_dict['prd1_sum'] = dataf_test1['diff'].sum()
        res_dict['prd1_mean'] = dataf_test1['diff'].mean()
        dataf_test1.drop(['FXSYL_prd', 'diff'], axis=1)

    if dataf_test2 is not None:
        dataf_test2['FXSYL_prd'] = model.predict(dataf_test2)
        dataf_test2['diff'] = dataf_test2['FXSYL'] - dataf_test2['FXSYL_prd']
        res_dict['prd2_sum'] = dataf_test2['diff'].sum()
        res_dict['prd2_mean'] = dataf_test2['diff'].mean()
        dataf_test2.drop(['FXSYL_prd', 'diff'], axis=1)

    if dataf_test3 is not None:
        dataf_test3['FXSYL_prd'] = model.predict(dataf_test3)
        dataf_test3['diff'] = dataf_test3['FXSYL'] - dataf_test3['FXSYL_prd']
        res_dict['prd3_sum'] = dataf_test3['diff'].sum()
        res_dict['prd3_mean'] = dataf_test3['diff'].mean()
        dataf_test3.drop(['FXSYL_prd', 'diff'], axis=1)

    if dataf_test4 is not None:
        dataf_test4['FXSYL_prd'] = model.predict(dataf_test4)
        dataf_test4['diff'] = dataf_test4['FXSYL'] - dataf_test4['FXSYL_prd']
        res_dict['prd4_sum'] = dataf_test4['diff'].sum()
        res_dict['prd4_mean'] = dataf_test4['diff'].mean()
        dataf_test4.drop(['FXSYL_prd', 'diff'], axis=1)

    return res_dict


def reg_model_d1(dataf, dataf_test1=None, dataf_test2=None, dataf_test3=None, dataf_test4=None):
    res_dict = {}
    model_d1 = 'FXSYL ~ S_RP1'
    ml = ols(model_d1, data=dataf).fit()

    res_dict['Intercept'] = ml.params.Intercept
    res_dict['coef_s_rp1'] = ml.params.S_RP1
    res_dict = get_variables_of_model(ml, res_dict)
    res_dict = get_variables_of_predict(ml, res_dict, dataf_test1, dataf_test2, dataf_test3, dataf_test4)

    # print(res_dict)
    # print(ml.params)
    # print(ml.summary())
    return res_dict


def reg_model_d2(dataf, dataf_test1=None, dataf_test2=None, dataf_test3=None, dataf_test4=None):
    res_dict = {}
    model_d2 = 'FXSYL ~ S_RP2'
    ml = ols(model_d2, data=dataf).fit()

    res_dict['Intercept'] = ml.params.Intercept
    res_dict['coef_s_rp2'] = ml.params.S_RP2
    res_dict = get_variables_of_model(ml, res_dict)
    res_dict = get_variables_of_predict(ml, res_dict, dataf_test1, dataf_test2, dataf_test3, dataf_test4)

    # print(res_dict)
    # print(ml.params)
    # print(ml.summary())
    return res_dict


def reg_model_s1(dataf, dataf_test1=None, dataf_test2=None, dataf_test3=None, dataf_test4=None):
    res_dict = {}
    model_s1 = 'FXSYL ~ S_RP1 + S_SMB1 + S_HML1'
    ml = ols(model_s1, data=dataf).fit()

    res_dict['Intercept'] = ml.params.Intercept
    res_dict['coef_s_rp1'] = ml.params.S_RP1
    res_dict['coef_s_smb1'] = ml.params.S_SMB1
    res_dict['coef_s_hml1'] = ml.params.S_HML1

    res_dict = get_variables_of_model(ml, res_dict)
    res_dict = get_variables_of_predict(ml, res_dict, dataf_test1, dataf_test2, dataf_test3, dataf_test4)

    # print(res_dict)
    # print(ml.params)
    # print(ml.summary())
    return res_dict


def reg_model_s2(dataf, dataf_test1=None, dataf_test2=None, dataf_test3=None, dataf_test4=None):
    res_dict = {}
    model_s2 = 'FXSYL ~ S_RP2 + S_SMB2 + S_HML2'
    ml = ols(model_s2, data=dataf).fit()

    res_dict['Intercept'] = ml.params.Intercept
    res_dict['coef_s_rp2'] = ml.params.S_RP2
    res_dict['coef_s_smb2'] = ml.params.S_SMB2
    res_dict['coef_s_hml2'] = ml.params.S_HML2

    res_dict = get_variables_of_model(ml, res_dict)
    res_dict = get_variables_of_predict(ml, res_dict, dataf_test1, dataf_test2, dataf_test3, dataf_test4)

    # print(res_dict)
    # print(ml.params)
    # print(ml.summary())
    return res_dict


def reg_model_w1(dataf, dataf_test1=None, dataf_test2=None, dataf_test3=None, dataf_test4=None):
    res_dict = {}
    model_w1 = 'FXSYL ~ W_RP1 + W_SMB1 + W_HML1 + W_RMW1 + W_CMA1'
    ml = ols(model_w1, data=dataf).fit()

    res_dict['Intercept'] = ml.params.Intercept
    res_dict['coef_w_rp1'] = ml.params.W_RP1
    res_dict['coef_w_smb1'] = ml.params.W_SMB1
    res_dict['coef_w_hml1'] = ml.params.W_HML1
    res_dict['coef_w_rmw1'] = ml.params.W_RMW1
    res_dict['coef_w_cma1'] = ml.params.W_CMA1

    res_dict = get_variables_of_model(ml, res_dict)
    res_dict = get_variables_of_predict(ml, res_dict, dataf_test1, dataf_test2, dataf_test3, dataf_test4)

    # print(res_dict)
    # print(ml.params)
    # print(ml.summary())
    return res_dict


def reg_model_w2(dataf, dataf_test1=None, dataf_test2=None, dataf_test3=None, dataf_test4=None):
    res_dict = {}
    model_w2 = 'FXSYL ~ W_RP2 + W_SMB2 + W_HML2 + W_RMW2 + W_CMA2'
    ml = ols(model_w2, data=dataf).fit()

    res_dict['Intercept'] = ml.params.Intercept
    res_dict['coef_w_rp2'] = ml.params.W_RP2
    res_dict['coef_w_smb2'] = ml.params.W_SMB2
    res_dict['coef_w_hml2'] = ml.params.W_HML2
    res_dict['coef_w_rmw2'] = ml.params.W_RMW2
    res_dict['coef_w_cma2'] = ml.params.W_CMA2

    res_dict = get_variables_of_model(ml, res_dict)
    res_dict = get_variables_of_predict(ml, res_dict, dataf_test1, dataf_test2, dataf_test3, dataf_test4)

    # print(res_dict)
    # print(ml.params)
    # print(ml.summary())
    return res_dict


def get_dataset_by_timespan(db, gpdm, start_dt, end_dt=None, include_ahead=True, order="ASC"):
    """
    根据开始时间和结束时间获取股票在该区间的历史日记录
    :param db: db连接池
    :param gpdm: 股票代码
    :param start_dt: 开始时间
    :param end_dt: 结束时间（可选）若未取结束时间，则从开始时间取到序列结束
    :param include_ahead: 如果True则包含前一时间点，False则包含后一时间点
    :param order: 数据顺序
    :return:
    """
    conn = db.connection()
    cur = conn.cursor()
    if end_dt is not None:
        if include_ahead:
            sql = \
                "select h.gpdm,h.dt,h.syl,h.dssyl,s.wfxll,s.riskpremium1,s.smb1,s.hml1,s.riskpremium2,s.smb2,s.hml2," \
                "w.riskpremium1,w.smb1,w.hml1,w.rmw1,w.cma1,w.riskpremium2,w.smb2,w.hml2,w.rmw2,w.cma2 " \
                "from history_price h " \
                "left join syzmx s on h.dt = s.dt " \
                "left join wyzmx w on h.dt = w.dt " \
                "where s.gpsclx = 'P9709' and w.tzzhlx = 1 and gpdm = '{}' " \
                "and h.dt >= '{}' and h.dt < '{}' " \
                "having s.smb1 is not null and w.smb1 is not null order by dt {};".format(gpdm, start_dt, end_dt, order)
        else:
            sql = \
                "select h.gpdm,h.dt,h.syl,h.dssyl,s.wfxll,s.riskpremium1,s.smb1,s.hml1,s.riskpremium2,s.smb2,s.hml2," \
                "w.riskpremium1,w.smb1,w.hml1,w.rmw1,w.cma1,w.riskpremium2,w.smb2,w.hml2,w.rmw2,w.cma2 " \
                "from history_price h " \
                "left join syzmx s on h.dt = s.dt " \
                "left join wyzmx w on h.dt = w.dt " \
                "where s.gpsclx = 'P9709' and w.tzzhlx = 1 and gpdm = '{}' " \
                "and h.dt > '{}' and h.dt <= '{}' " \
                "having s.smb1 is not null and w.smb1 is not null order by dt {};".format(gpdm, start_dt, end_dt, order)
    else:
        if include_ahead:
            sql = \
                "select h.gpdm,h.dt,h.syl,h.dssyl,s.wfxll,s.riskpremium1,s.smb1,s.hml1,s.riskpremium2,s.smb2,s.hml2," \
                "w.riskpremium1,w.smb1,w.hml1,w.rmw1,w.cma1,w.riskpremium2,w.smb2,w.hml2,w.rmw2,w.cma2 " \
                "from history_price h " \
                "left join syzmx s on h.dt = s.dt " \
                "left join wyzmx w on h.dt = w.dt " \
                "where s.gpsclx = 'P9709' and w.tzzhlx = 1 and gpdm = '{}' " \
                "and h.dt >= '{}'  " \
                "having s.smb1 is not null and w.smb1 is not null order by dt {};".format(gpdm, start_dt, order)
        else:
            sql = \
                "select h.gpdm,h.dt,h.syl,h.dssyl,s.wfxll,s.riskpremium1,s.smb1,s.hml1,s.riskpremium2,s.smb2,s.hml2," \
                "w.riskpremium1,w.smb1,w.hml1,w.rmw1,w.cma1,w.riskpremium2,w.smb2,w.hml2,w.rmw2,w.cma2 " \
                "from history_price h " \
                "left join syzmx s on h.dt = s.dt " \
                "left join wyzmx w on h.dt = w.dt " \
                "where s.gpsclx = 'P9709' and w.tzzhlx = 1 and gpdm = '{}' " \
                "and h.dt > '{}'  " \
                "having s.smb1 is not null and w.smb1 is not null order by dt {};".format(gpdm, start_dt, order)
    # print(sql)
    cur.execute(sql)
    dataset = []
    for item in cur.fetchall():
        line = []
        for i in item:
            line.append(i)
        dataset.append(line)
    cur.close()
    conn.close()
    return dataset


def generate_sql_to_insert(gpdm, dt, model_name, regset, res_dict):
    sql = "insert into reg_res(gpdm, dt, model, regset, var, value) VALUES "
    for key, value in res_dict.items():
        sql = sql + " ('{}','{}','{}','{}','{}',{}),".format(gpdm, dt, model_name, regset, key, value)
    sql = sql[:-1] + ';'
    return sql


def process_event(db, gpdm, e_date):
    print("{} - {}:开始分析".format(gpdm, e_date))

    "获取股票历史时间节点信息"
    # 获取事件前后最近的时间戳
    e_dtid_before = timeline.get_dtid_before_by_dt(db, e_date)
    e_dtid_after = timeline.get_dtid_after_by_dt(db, e_date)

    # 获取事件后节点信息
    dt2_0 = timeline.get_dt_by_dtid(db, e_dtid_after)
    dt2_10 = timeline.get_dt_by_dtid(db, e_dtid_after + 10)
    dt2_20 = timeline.get_dt_by_dtid(db, e_dtid_after + 20)
    dt2_40 = timeline.get_dt_by_dtid(db, e_dtid_after + 40)
    dt2_240 = timeline.get_dt_by_dtid(db, e_dtid_after + 240)
    # 获取事件后各区段数据
    dataset2_10_5 = get_dataset_by_timespan(db, gpdm, dt2_0, dt2_10, True, 'ASC')
    dataset2_20_10 = get_dataset_by_timespan(db, gpdm, dt2_0, dt2_20, True, 'ASC')
    dataset2_40_20 = get_dataset_by_timespan(db, gpdm, dt2_0, dt2_40, True, 'ASC')
    dataset2_240_120 = get_dataset_by_timespan(db, gpdm, dt2_0, dt2_240, True, 'ASC')     # 若dt2_240超出限制，则自动取到可取到历史的结尾

    # 获取事件前节点时间
    dt_5 = timeline.get_dt_by_dtid(db, e_dtid_before - 5)
    dt_125 = timeline.get_dt_by_dtid(db, e_dtid_before - 125)
    dt_10 = timeline.get_dt_by_dtid(db, e_dtid_before - 10)
    dt_130 = timeline.get_dt_by_dtid(db, e_dtid_before - 130)
    dt_20 = timeline.get_dt_by_dtid(db, e_dtid_before - 20)
    dt_140 = timeline.get_dt_by_dtid(db, e_dtid_before - 140)
    # 获取事件前各区段数据
    dataset_5_125 = get_dataset_by_timespan(db, gpdm, dt_125, dt_5, False, 'DESC')
    dataset_10_130 = get_dataset_by_timespan(db, gpdm, dt_130, dt_10, False, 'DESC')
    dataset_20_140 = get_dataset_by_timespan(db, gpdm, dt_140, dt_20, False, 'DESC')

    # 输出各区段数据
    print('--------dataset2_10_5--------')
    print(dataset2_10_5)
    print('--------dataset2_20_10--------')
    print(dataset2_20_10)
    print('--------dataset2_40_20--------')
    print(dataset2_40_20)
    print('--------dataset2_240_120--------')
    print(dataset2_240_120)

    print('--------dataset_5_125--------')
    print(dataset_5_125)
    print('--------dataset_10_130--------')
    print(dataset_10_130)
    print('--------dataset_20_140--------')
    print(dataset_20_140)

    # 检查各预测区间数据可用性 并转为dataframe,如果预测集不符合要求，则置为None
    # a:5/10; b:10/20; c:20/40; d:120/240
    if dataset2_10_5 is not None and len(dataset2_10_5) >= 5:
        dataset2_10_5 = dataset2_10_5[:5]
        dataf_a = pd.DataFrame(dataset2_10_5, columns=dataset_header[0])
        dataf_a['FXSYL'] = dataf_a['SYL'] - dataf_a['WFXLL']
    else:
        dataf_a = None

    if dataset2_20_10 is not None and len(dataset2_20_10) >= 10:
        dataset2_20_10 = dataset2_20_10[:10]
        dataf_b = pd.DataFrame(dataset2_20_10, columns=dataset_header[0])
        dataf_b['FXSYL'] = dataf_b['SYL'] - dataf_b['WFXLL']
    else:
        dataf_b = None

    if dataset2_40_20 is not None and len(dataset2_40_20) >= 20:
        dataset2_40_20 = dataset2_40_20[:20]
        dataf_c = pd.DataFrame(dataset2_40_20, columns=dataset_header[0])
        dataf_c['FXSYL'] = dataf_c['SYL'] - dataf_c['WFXLL']
    else:
        dataf_c = None

    if dataset2_240_120 is not None and len(dataset2_240_120) >= 120:
        dataset2_240_120 = dataset2_240_120[:120]
        dataf_d = pd.DataFrame(dataset2_240_120, columns=dataset_header[0])
        dataf_d['FXSYL'] = dataf_d['SYL'] - dataf_d['WFXLL']
    else:
        dataf_d = None

    # 判断事件前各区段数据可用性，如果可用则对该区段进行分析
    # A:5~125 B:10~130 C:20~140
    sql_set = []    # 存放待执行的sql语句

    if dataset_5_125 is not None and len(dataset_5_125) >= 110:
        dataf_A = pd.DataFrame(dataset_5_125, columns=dataset_header[0])
        dataf_A['FXSYL'] = dataf_A['SYL'] - dataf_A['WFXLL']
        res_A_d1 = reg_model_d1(dataf_A, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 'd1', 'A', res_A_d1))
        res_A_d2 = reg_model_d2(dataf_A, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 'd2', 'A', res_A_d2))
        res_A_s1 = reg_model_s1(dataf_A, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 's1', 'A', res_A_s1))
        res_A_s2 = reg_model_s2(dataf_A, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 's2', 'A', res_A_s2))
        res_A_w1 = reg_model_w1(dataf_A, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 'w1', 'A', res_A_w1))
        res_A_w2 = reg_model_w2(dataf_A, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 'w2', 'A', res_A_w2))

    if dataset_10_130 is not None and len(dataset_10_130) >= 110:
        dataf_B = pd.DataFrame(dataset_10_130, columns=dataset_header[0])
        dataf_B['FXSYL'] = dataf_B['SYL'] - dataf_B['WFXLL']
        res_B_d1 = reg_model_d1(dataf_B, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 'd1', 'B', res_B_d1))
        res_B_d2 = reg_model_d2(dataf_B, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 'd2', 'B', res_B_d2))
        res_B_s1 = reg_model_s1(dataf_B, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 's1', 'B', res_B_s1))
        res_B_s2 = reg_model_s2(dataf_B, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 's2', 'B', res_B_s2))
        res_B_w1 = reg_model_w1(dataf_B, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 'w1', 'B', res_B_w1))
        res_B_w2 = reg_model_w2(dataf_B, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 'w2', 'B', res_B_w2))

    if dataset_20_140 is not None and len(dataset_20_140) >= 110:
        dataf_C = pd.DataFrame(dataset_20_140, columns=dataset_header[0])
        dataf_C['FXSYL'] = dataf_C['SYL'] - dataf_C['WFXLL']
        res_C_d1 = reg_model_d1(dataf_C, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 'd1', 'C', res_C_d1))
        res_C_d2 = reg_model_d2(dataf_C, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 'd2', 'C', res_C_d2))
        res_C_s1 = reg_model_s1(dataf_C, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 's1', 'C', res_C_s1))
        res_C_s2 = reg_model_s2(dataf_C, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 's2', 'C', res_C_s2))
        res_C_w1 = reg_model_w1(dataf_C, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 'w1', 'C', res_C_w1))
        res_C_w2 = reg_model_w2(dataf_C, dataf_a, dataf_b, dataf_c, dataf_d)
        sql_set.append(generate_sql_to_insert(gpdm, e_date, 'w2', 'C', res_C_w2))

    # 全部回归结束，在event_list中添加标记
    sql_flag = "update event_list set is_reg = 1 where gpdm = '{}' and dt = '{}';".format(gpdm, e_date)
    sql_set.append(sql_flag)
    conn = db.connection()
    cur = conn.cursor()
    try:
        for sql in sql_set:
            pass
            cur.execute(sql)
        conn.commit()
    except Exception as e:
        err_str = "{} - {}:".format(gpdm, e_date) + str(e)
        return err_str
    finally:
        cur.close()
        conn.close()


def get_event_list(db):
    """
    获取股票历史事件列表
    :param db: 数据库连接池
    :return: [['股票代码','事件日期']...]
    """
    conn = db.connection()
    cur = conn.cursor()
    sql = "select gpdm, dt from event_list where seq!= 1 and is_reg = 0;"
    cur.execute(sql)
    event_list = []
    for item in cur.fetchall():
        event_list.append([item[0], item[1]])
    cur.close()
    conn.close()
    return event_list


def pool_action(thread_name, db, gpdm, e_date):
    try:
        res = process_event(db, gpdm, e_date)
        return res
    except Exception as e:
        err_str = "{} - {}:".format(gpdm, e_date) + str(e)
        return err_str


def callback(status, result):
    print(result)


def run():
    db = db_pool.get_db_pool(True)     # 初始化数据库连接池
    event_list = get_event_list(db)     # 获取等待处理的发行列表
    pool = thread_pool.ThreadPool(10)

    for item in event_list:
        print(item)
        pool.put(pool_action, (db, item[0], item[1],), callback)

    while True:
        time.sleep(0.5)
        if len(pool.generate_list) - len(pool.free_list) == 0:
            print("Task finished! Closing...")
            pool.close()
            break
        else:
            # print("{} Threads still working.Wait.".format(len(pool.generate_list) - len(pool.free_list)))
            pass


if __name__ == '__main__':
    run()

    # db = db_pool.get_db_pool(False)
    # process_event(db, '000590', '2017-05-24')

