#!/bin/bash
APP=edu

if [ -n "$2" ] ;then
   do_date=$2
else
   echo "请传入日期参数"
   exit
fi

dws_trade_user_order_td="

insert overwrite table ${APP}.dws_trade_user_order_td partition(dt='$do_date')
select
    user_id,
    min(dt) order_date_first,
    max(dt) order_date_last,
    sum(order_count_1d) order_count,
    sum(order_original_amount_1d) original_amount,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount,
    sum(order_total_amount_1d) total_amount
from ${APP}.dws_trade_user_order_1d
 where dt = '$do_date'
group by user_id;
"

dws_user_user_login_td="

insert overwrite table ${APP}.dws_user_user_login_td partition (dt = '$do_date')
select u.id                                                         user_id,
       nvl(login_date_last, date_format(create_time, 'yyyy-MM-dd')) login_date_last,
       date_format(create_time, 'yyyy-MM-dd')                       login_date_first,
       nvl(login_count_td, 1)                                       login_count_td
from (
         select id,
                create_time
         from ${APP}.dim_user_zip
         where dt = '9999-12-31'
     ) u
         left join
     (
         select user_id,
                max(dt)  login_date_last,
                count(*) login_count_td
         from ${APP}.dwd_user_login_inc
         group by user_id
     ) l
     on u.id = l.user_id;
"
dws_trade_user_payment_td="

insert overwrite table ${APP}.dws_trade_user_payment_td partition(dt='$do_date')
select user_id,
       min(payment_date_first)          payment_date_first,
       max(payment_date_last)           payment_date_last,
       sum(payment_count_td)            payment_count_td,
       sum(payment_amount_td)           payment_amount_td
from (
         select user_id,
                payment_date_first,
                payment_date_last,
                payment_count_td,
                payment_amount_td
         from ${APP}.dws_trade_user_payment_td
         where dt = date_add('$do_date', -1)
         union all
         select user_id,
                '$do_date' payment_date_first,
                '$do_date' payment_date_last,
                payment_count_1d,
                payment_amount_1d
         from ${APP}.dws_trade_user_payment_1d
         where dt = '$do_date') t1
group by user_id;
"

case $1 in
    "dws_trade_user_order_td" )
        hive -e "$dws_trade_user_order_td"
    ;;
    "dws_user_user_login_td" )
        hive -e "$dws_user_user_login_td"
    ;;
    "dws_trade_user_payment_td" )
        hive -e "$dws_trade_user_payment_td"
    ;;
    "all" )
        hive -e "$dws_trade_user_order_td$dws_user_user_login_td$dws_trade_user_payment_td"
    ;;
esac
