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
drop table if exists gghslb;
create table gghslb(
    dt date comment '交易日期',
    zqdm char(6) comment '证券代码',
    tcbz tinyint comment '填充标识(2不计算)',
    sclx tinyint comment '市场类型 - 1=上海A，2=上海B，4=深圳A，8=深圳B, 16=创业板，32=科创板',
    csz smallint comment '参数值',
    csdw varchar(5) comment '参数单位',
    hsl_zgb float comment '换手率(总股本)',
    hsl_ltgb float comment '换手率(流通股本)'
)comment '个股换手率表';

show tables;

select * from gghslb;

delete from gghslb where csz = 1;

select csz, count(*) from gghslb
group by csz;

select distinct csdw from gghslb;
select distinct csz from gghslb;

delete from gghslb where csz != 1;

select distinct dt from gghslb
where dt not in (
    select dt from szzz
    );

select distinct dt from szzz
where dt not in (
    select distinct dt from gghslb
    );

select distinct gpdm from event_list
where gpdm not in (
    select distinct gpdm from gghslb
    );

select distinct zjhhy from event_list order by zjhhy;
select distinct windhy from event_list order by windhy;
select * from event_list where zjhhy is Null;
select * from event_list where gpdm in (
    select gpdm from event_list where zjhhy is null
    ) order by gpdm;


show create table event_list;

alter table event_list add column hydm char(2) comment '证监会行业代码(A~S)' after zjhhy;

select * from event_list
where zjhhy like '交通运输、仓储业-%';


select count(*) from event_list;
select count(*) from event_list where zjhhy is Null;
select count(*) from event_list where zjhhy like '%*';