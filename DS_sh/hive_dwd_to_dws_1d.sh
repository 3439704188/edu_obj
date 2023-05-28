#!/bin/bash
APP=edu

if [ -n "$2" ] ;then
   do_date=$2
else
   echo "请传入日期参数"
   exit
fi

dws_trade_user_course_order_1d="

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled = false;
insert overwrite table ${APP}.dws_trade_user_course_order_1d partition(dt='$do_date')
select
    user_id,
    course_id,
    course_name,
    category_id,
    category_name,
    subject_id,
    subject_name,
    order_count_1d,
    order_original_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d
from
(
    select
        dt,
        user_id,
        course_id,
        count(*) order_count_1d,
        sum(origin_amount) order_original_amount_1d,
        sum(nvl(coupon_reduce,0.0)) coupon_reduce_amount_1d,
        sum(final_amount) order_total_amount_1d
    from ${APP}.dwd_trade_order_detail_inc
    group by dt,user_id,course_id
)od
left join
(
    select
        id,
        course_name,
        category_id,
        category_name,
        subject_id,
        subject_name
    from ${APP}.dim_course_full
    where dt='$do_date'
)course
on od.course_id=course.id;
set hive.vectorized.execution.enabled = true;
"

dws_trade_user_order_1d="
insert overwrite table ${APP}.dws_trade_user_order_1d partition(dt='$do_date')
select
    user_id,
    order_count_1d,
    order_original_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d
from
(
    select
        user_id,
        count(*) order_count_1d,
        sum(origin_amount) order_original_amount_1d,
        sum(if(coupon_reduce is not null,coupon_reduce,0.0)) coupon_reduce_amount_1d,
        sum(final_amount) order_total_amount_1d
    from ${APP}.dwd_trade_order_detail_inc
    where dt='$do_date'
    group by user_id
)od;
"
dws_trade_user_cart_add_1d="

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dws_trade_user_cart_add_1d partition(dt='$do_date')
select
    user_id,
    count(*)
from ${APP}.dwd_trade_cart_add_inc
    where dt='$do_date'
group by user_id;
"

dws_trade_user_payment_1d="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dws_trade_user_payment_1d partition(dt='$do_date')
select
    user_id,
    count(distinct(order_id)),
    sum(payment_amount)
from ${APP}.dwd_trade_pay_detail_suc_inc
    where dt='$do_date'
group by user_id;
"

dws_trade_province_order_1d="

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dws_trade_province_order_1d partition(dt='$do_date')
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_user_count_1d,
    order_original_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d
from
(
    select
        province_id,
        count(distinct(order_id)) order_count_1d,
        count(distinct(user_id)) order_user_count_1d,
        sum(origin_amount) order_original_amount_1d,
        sum(nvl(coupon_reduce,0)) coupon_reduce_amount_1d,
        sum(final_amount) order_total_amount_1d
    from ${APP}.dwd_trade_order_detail_inc
        where dt='$do_date'
    group by province_id
)o
left join
(
    select
        id,
        province_name,
        area_code,
        iso_code,
        iso_3166_2
    from ${APP}.dim_province_full
    where dt='$do_date'
)p
on o.province_id=p.id;
"

dws_interaction_course_favor_add_1d="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dws_interaction_course_favor_add_1d partition(dt='$do_date')
select
    course_id,
    course_name,
    category_id,
    category_name,
    subject_id,
    subject_name,
    favor_add_count
from
(
    select
        course_id,
        count(*) favor_add_count
    from ${APP}.dwd_interaction_favor_add_inc
    where dt='$do_date'
    group by course_id
)favor
left join
(
    select
    	id,
        course_name,
        category_id,
        category_name,
        subject_id,
        subject_name
    from ${APP}.dim_course_full
    where dt='$do_date'
)course
on favor.course_id=course.id;
"

dws_course_review_info_1d="

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dws_course_review_info_1d partition(dt='$do_date')
select
    course_id  ,
    course_name ,
    user_count ,
    user_count_5,
    review_sum
from
(
    select
        course_id,
        count(distinct user_id) as user_count  ,
        count(distinct if(review_stars=5,user_id,null) )user_count_5  ,
        sum(review_stars)review_sum
    from ${APP}.dwd_interaction_review_info_inc
    where dt='$do_date'
    group by course_id
) review
left join
(
    select
    	id,
        course_name
    from ${APP}.dim_course_full
    where dt='$do_date'
)course
on review.course_id=course.id;
"


dws_traffic_session_page_view_1d="

insert overwrite table ${APP}.dws_traffic_session_page_view_1d partition(dt='$do_date')
select
    session_id,
    mid_id,
    source_id,
    brand,
    model,
    operate_system,
    version_code,
    channel,
    sum(during_time),
    count(*)
from ${APP}.dwd_traffic_page_view_inc
where dt='$do_date'
group by session_id,mid_id,source_id,brand,model,operate_system,version_code,channel;
"

dws_traffic_page_visitor_page_view_1d="

insert overwrite table ${APP}.dws_traffic_page_visitor_page_view_1d partition(dt='$do_date')
select
    mid_id,
    source_id,
    brand,
    model,
    operate_system,
    page_id,
    sum(during_time),
    count(*)
from ${APP}.dwd_traffic_page_view_inc
where dt='$do_date'
group by mid_id,source_id,brand,model,operate_system,page_id;
"

dws_complete_user_chapter_first_date_1d="

insert overwrite table ${APP}.dws_complete_user_chapter_first_date_1d partition(dt='$do_date')
select
       user_id,
       chapter_id,
       course_id,
       subject_id,
       create_time first_complete_dt_1d
from
(
select
       user_id,
       chapter_id,
       course_id,
       subject_id,
       video_id,
       max(position_sec) max_position_sec,
       sum(position_sec) total_play_sec,
       create_time
from ${APP}.dwd_consume_video_play_inc
where dt='$do_date'
group by user_id, chapter_id, course_id, subject_id,video_id,create_time

)p join
(
select
       id,
       during_sec
from ${APP}.dim_video_full
where dt = '$do_date'
)v on p.video_id = v.id
where max_position_sec / during_sec >= 0.9 and
total_play_sec/during_sec >= 0.9;
"


dws_consume_chapter_play_video_1d="

insert overwrite table ${APP}.dws_consume_chapter_play_video_1d partition(dt='$do_date')
select
       chapter_id,
       chapter_name,
       video.course_id,
       co.course_name,
       video.subject_id,
       subject_name,
       video.category_id,
       category_name,
       sum(position_sec)       position_sec_1d,
       count(distinct if(is_shot=1,user_id,null)),
       count(distinct user_id) user_count_1d,
       count(1)                view_count_1d
from (select * from ${APP}.dwd_consume_video_play_inc
where dt = '$do_date'
) video
left join(select * from ${APP}.dim_course_full
where dt='$do_date' ) co
on video.course_id=co.id
left join(select * from ${APP}.dim_chapter_full
where dt='$do_date' ) ch
on video.chapter_id=ch.id
group by        chapter_id,
       chapter_name,
       video.course_id,
       co.course_name,
       video.subject_id,
       subject_name,
       video.category_id,
       category_name;
"


dws_test_paper_exam_1d="

insert overwrite table ${APP}.dws_test_paper_exam_1d partition(dt='$do_date')
select
       user_id,
       paper_id,
       course_id,
       subject_id,
       sum(score)              score_sum_1d,
       sum(duration_sec)       during_time_1d,
       count(distinct user_id) user_count_1d
from ${APP}.dwd_consume_test_exam_inc
where dt = '$do_date'
group by user_id, paper_id, course_id, subject_id;
"

dws_complete_user_course_first_date_1d="

insert overwrite table ${APP}.dws_complete_user_course_first_date_1d partition(dt='$do_date')
select
       user_id,
       course_id,
       subject_id,
       complete_time first_complete_dt_1d
from
(
select
       user_id,
       count(distinct chapter_id) finish,
       course_id,
       subject_id,
       max(first_complete_dt_1d) complete_time
from ${APP}.dws_complete_user_chapter_first_date_1d
where dt = '$do_date'
group by user_id, course_id, subject_id
)p right join
(
select
       id,
       chapter_num
from ${APP}.dim_course_full
where dt = '$do_date'
)v on p.course_id = v.id
where finish=v.chapter_num;
"

dws_complete_course_1d="

insert overwrite table ${APP}.dws_complete_course_1d partition(dt='$do_date')
select
       course_id,
       subject_id,
       finish,
       complete_peo
from
(
select
       course_id,
       subject_id,
       count(user_id) finish,
       count(distinct user_id) complete_peo
from ${APP}.dws_complete_user_course_first_date_1d
where dt='$do_date'
group by course_id, subject_id
)p;
"


dws_test_paper_exam_1d="

insert overwrite table ${APP}.dws_test_paper_exam_1d partition(dt='$do_date')
select
       paper_id,
       course_id,
       subject_id,
       sum(score)              score_sum_1d,
       sum(duration_sec)       during_time_1d,
       count(distinct user_id) user_count_1d
from ${APP}.dwd_consume_test_exam_inc
where dt = '$do_date'
group by paper_id, course_id, subject_id;
"


dws_test_answer_1d="

insert overwrite table ${APP}.dws_test_answer_1d partition (dt = '$do_date')
select question_id,
       tq.course_id,
       course_name,
       count(user_id)                           answer_count_1d,
       count(if(is_correct = 1, user_id, null))answer_correct_count_1d
from (select question_id,
             course_id,
             user_id,
             is_correct
      from ${APP}.dwd_consume_test_question_inc
      where dt = '$do_date'
     ) tq
         join
     (select id,
             course_name
      from ${APP}.dim_question_info_full
      where dt = '$do_date'
     ) qi
     on tq.question_id = qi.id
group by tq.course_id,question_id,course_name;
"

case $1 in
    "dws_trade_province_order_1d" )
        hive -e "$dws_trade_province_order_1d"
    ;;
    "dws_trade_user_cart_add_1d" )
        hive -e "$dws_trade_user_cart_add_1d"
    ;;
    "dws_trade_user_order_1d" )
        hive -e "$dws_trade_user_order_1d"
    ;;
    "dws_trade_user_payment_1d" )
        hive -e "$dws_trade_user_payment_1d"
    ;;
    "dws_trade_user_course_order_1d" )
        hive -e "$dws_trade_user_course_order_1d"
    ;;
    "dws_interaction_course_favor_add_1d" )
        hive -e "$dws_interaction_course_favor_add_1d"
    ;;
    "dws_course_review_info_1d" )
        hive -e "$dws_course_review_info_1d"
    ;;
    "dws_traffic_session_page_view_1d" )
        hive -e "$dws_traffic_session_page_view_1d"
    ;;
    "dws_traffic_page_visitor_page_view_1d" )
        hive -e "$dws_traffic_page_visitor_page_view_1d"
    ;;
    "dws_complete_user_chapter_first_date_1d" )
        hive -e "$dws_complete_user_chapter_first_date_1d"
    ;;
    "dws_consume_chapter_play_video_1d" )
        hive -e "$dws_consume_chapter_play_video_1d"
    ;;
    "dws_test_paper_exam_1d" )
        hive -e "$dws_test_paper_exam_1d"
    ;;
    "dws_complete_user_course_first_date_1d" )
        hive -e "$dws_complete_user_course_first_date_1d"
    ;;
    "dws_complete_course_1d" )
        hive -e "$dws_complete_course_1d"
    ;;
    "dws_test_paper_exam_1d" )
        hive -e "$dws_test_paper_exam_1d"
    ;;
    "dws_test_answer_1d" )
        hive -e "$dws_test_answer_1d"
    ;;
    "all" )
        hive -e "$dws_trade_province_order_1d$dws_trade_user_cart_add_1d$dws_trade_user_order_1d$dws_trade_user_payment_1d$dws_trade_user_course_order_1d$dws_interaction_course_favor_add_1d$dws_course_review_info_1d$dws_traffic_session_page_view_1d$dws_traffic_page_visitor_page_view_1d$dws_complete_user_chapter_first_date_1d$dws_consume_chapter_play_video_1d$dws_test_paper_exam_1d$dws_complete_user_course_first_date_1d$dws_complete_course_1d$dws_test_paper_exam_1d$dws_test_answer_1d$dws_trade_province_order_1d$dws_trade_user_cart_add_1d$dws_trade_user_order_1d$dws_trade_user_payment_1d$dws_trade_user_course_order_1d$dws_interaction_course_favor_add_1d$dws_course_review_info_1d$dws_traffic_session_page_view_1d$dws_traffic_page_visitor_page_view_1d$dws_complete_user_chapter_first_date_1d$dws_consume_chapter_play_video_1d$dws_test_paper_exam_1d$dws_complete_user_course_first_date_1d$dws_complete_course_1d$dws_test_paper_exam_1d$dws_test_answer_1d"
    ;;
    esac















