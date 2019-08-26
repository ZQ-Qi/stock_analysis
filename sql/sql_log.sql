# 剔除金融行业
# 缩尾处理 累计异常收益率的前10%和后10%剔除,T检验的时候不放进去
# 限制时间到2010年后，到2018年底


select zjhhy, count(zjhhy) from event_list
group by zjhhy;

select count(*) from event_list
where is_name_change != 2 and isipo = 0 and dt > '2010-01-01' and dt < '2019-01-01';
# 3283条增发事件

show create table event_list;

select * from event_list a
right join reg_res b on a.gpdm = b.gpdm and a.dt = b.dt
where a.gpdm is null;

select * from event_list
where zcxs like '联合证券%';

SELECT DISTINCT var FROM reg_res;

