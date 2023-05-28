#!/bin/bash
APP=edu

if [ -n "$2" ] ;then
   do_date=$2
else
   echo "请传入日期参数"
   exit
fi

dws_trade_user_course_order_nd="

insert overwrite table ${APP}.dws_trade_user_course_order_nd partition(dt='$do_date')
select
    user_id,
    course_id,
    course_name,
    category_id,
    category_name,
    subject_id,
    subject_name,
    sum(if(dt>=date_add('$do_date',-6),order_count_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_original_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from ${APP}.dws_trade_user_course_order_1d
where dt>=date_add('$do_date',-29)
group by  user_id,course_id,course_name,category_id,category_name,subject_id,subject_name;
"

dws_trade_province_order_nd="

insert overwrite table ${APP}.dws_trade_province_order_nd partition(dt='$do_date')
select
    p1.province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    a,
    b,
    c,
    d,
    e,
    f,
    g,
    h,
   i,
    j
    from
(select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(if(dt>=date_add('$do_date',-6),order_count_1d,0)) a,
    sum(if(dt>=date_add('$do_date',-6),order_original_amount_1d,0)) c,
    sum(if(dt>=date_add('$do_date',-6),coupon_reduce_amount_1d,0)) d,
    sum(if(dt>=date_add('$do_date',-6),order_total_amount_1d,0)) e,
    sum(order_count_1d) f,
    sum(order_original_amount_1d) h,
    sum(coupon_reduce_amount_1d) i,
    sum(order_total_amount_1d) j
from ${APP}.dws_trade_province_order_1d
where dt>=date_add('$do_date',-29)
and dt<='$do_date'
group by province_id,province_name,area_code,iso_code,iso_3166_2
) p1
left join (
    select
        province_id,
        count(distinct if(dt>=date_add('$do_date',-6),user_id,null)) b,
        count(distinct(user_id)) g,
        dt
    from ${APP}.dwd_trade_order_detail_inc
    where dt>date_add('$do_date',-29) and dt<='$do_date'
    group by province_id,dt
)o
on p1.province_id=o.province_id;
"
dws_consume_chapter_play_video_nd="

insert overwrite table ${APP}.dws_consume_chapter_play_video_nd partition(dt='$do_date')
select
    chapter_id   ,
    chapter_name  ,
    p1.course_id     ,
    co.course_name   ,
    p1.subject_id     ,
    subject_name  ,
    p1.category_id    ,
    category_name    ,
    position_sec_7d  ,
    shot_count_7d  ,
    user_count_7d  ,
    view_count_7d   ,
    position_sec_30d  ,
    shot_count_30d   ,
    user_count_30d   ,
    view_count_30d
    from
(select
    chapter_id,
    course_id,
    subject_id,
    category_id,
    sum(if(dt>=date_add('$do_date',-6),position_sec,0)) position_sec_7d ,
    count(distinct if(is_shot=1 and dt>=date_add('$do_date',-6),user_id,null)) shot_count_7d,
    count(distinct if(dt>=date_add('$do_date',-6),user_id,null)) user_count_7d,
    count(if(dt>=date_add('$do_date',-6),1,null)) view_count_7d,
    sum(position_sec) position_sec_30d,
    count(distinct if(is_shot=1,user_id,null)) shot_count_30d,
    count(distinct user_id) user_count_30d,
    count(1) view_count_30d
from ${APP}.dwd_consume_video_play_inc
where dt>=date_add('$do_date',-29)
and dt<='$do_date'
group by chapter_id,course_id,
        subject_id,category_id
) p1
left join(select * from ${APP}.dim_course_full
where dt='$do_date' ) co
on p1.course_id=co.id
left join(select * from ${APP}.dim_chapter_full
where dt='$do_date' ) ch
on p1.chapter_id=ch.id;
"

dws_trade_user_order_nd="

insert overwrite table ${APP}.dws_trade_user_order_nd partition(dt='$do_date')
select
    p1.user_id,
    a,
    c,
    d,
    e,
    f,
    h,
   i,
    j
    from
(select
    user_id,
    sum(if(dt>=date_add('$do_date',-6),order_count_1d,0)) a,
    sum(if(dt>=date_add('$do_date',-6),order_original_amount_1d,0)) c,
    sum(if(dt>=date_add('$do_date',-6),coupon_reduce_amount_1d,0)) d,
    sum(if(dt>=date_add('$do_date',-6),order_total_amount_1d,0)) e,
    sum(order_count_1d) f,
    sum(order_original_amount_1d) h,
    sum(coupon_reduce_amount_1d) i,
    sum(order_total_amount_1d) j
from ${APP}.dws_trade_user_order_1d
where dt>=date_add('$do_date',-29)
and dt<='$do_date'
group by user_id
) p1;
"

dws_complete_course_nd="

insert overwrite table ${APP}.dws_complete_course_nd partition(dt='$do_date')
select
       p.course_id,
       p.subject_id,
       finish7,
       complete_peo7,
       finish30,
       complete_peo30
from
(
select
       course_id,
       subject_id,
       sum(if(dt>=date_add('$do_date',-6),complete_count_1d,0)) finish7,
        sum(complete_count_1d) finish30
from ${APP}.dws_complete_course_1d
where dt>=date_add('$do_date',-29)
and dt<='$do_date'
group by course_id, subject_id
)p
join
(
    select
       course_id,
       subject_id,
       count(distinct if(dt>=date_add('$do_date',-6),user_id,null)) complete_peo7,
       count(distinct user_id) complete_peo30
from ${APP}.dws_complete_user_course_first_date_1d
where dt>=date_add('$do_date',-29)
and dt<='$do_date'
group by course_id, subject_id
    ) c
on p.course_id =c.course_id;
"

dws_complete_user_chapter_nd="

insert overwrite table ${APP}.dws_complete_user_chapter_nd partition(dt='$do_date')
select
       user_id,
       chapter_id,
       course_id,
       course_name,
       subject_id,
       complete_7d,
       complete_30d
from
(
select
       user_id,
       chapter_id,
       course_id,
       subject_id,
       count(if(dt>=date_add('$do_date',-6),first_complete_dt_1d,null)) complete_7d,
        count(first_complete_dt_1d) complete_30d
from ${APP}.dws_complete_user_chapter_first_date_1d
where dt>=date_add('$do_date',-29)
and dt<='$do_date'
group by user_id, chapter_id, course_id, subject_id
)p join
(
select
       id,
       course_name
from ${APP}.dim_course_full
where dt = '$do_date'
)v on p.course_id = v.id;
"


dws_course_review_info_nd="

insert overwrite table ${APP}.dws_course_review_info_nd partition(dt='$do_date')
select
    course_id,
    course_name,
    sum(if(dt>=date_add('$do_date',-6),user_count,0)),
    sum(if(dt>=date_add('$do_date',-6),user_count_5,0)),
    sum(if(dt>=date_add('$do_date',-6),review_sum,0)),
    sum(user_count),
    sum(user_count_5),
    sum(review_sum)
from ${APP}.dws_course_review_info_1d
where dt>=date_add('$do_date',-29)
group by  course_id,course_name;
"

dws_test_paper_exam_nd="

insert overwrite table ${APP}.dws_test_paper_exam_nd partition(dt='$do_date')
select
    paper_id,
    course_id,
    subject_id,
    sum(if(dt>=date_add('$do_date',-6),score_sum_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),during_time_1d,0)),
    sum(if(dt>=date_add('$do_date',-6),user_count_1d,0)),
    sum(score_sum_1d),
    sum(during_time_1d),
    sum(user_count_1d)
from ${APP}.dws_test_paper_exam_1d
where dt>=date_add('$do_date',-29)
group by  course_id,course_id,subject_id,paper_id;
"

dws_test_answer_nd="

insert overwrite table ${APP}.dws_test_answer_nd partition(dt='$do_date')
select
    question_id     ,
    course_id       ,
    course_name     ,
    count(if(dt>=date_add('$do_date',-6),answer_count_1d,null)),
    count(if(dt>=date_add('$do_date',-6),answer_correct_count_1d,null)),
    count(answer_count_1d),
    count(answer_correct_count_1d)
from ${APP}.dws_test_answer_1d
where dt>=date_add('$do_date',-29)
group by  question_id,course_id,course_name;
"

case $1 in
    "dws_trade_user_course_order_nd" )
        hive -e "$dws_trade_user_course_order_nd"
    ;;
    "dws_trade_province_order_nd" )
        hive -e "$dws_trade_province_order_nd"
    ;;
    "dws_consume_chapter_play_video_nd" )
        hive -e "$dws_consume_chapter_play_video_nd"
    ;;
    "dws_trade_user_order_nd" )
        hive -e "$dws_trade_user_order_nd"
    ;;
    "dws_complete_course_nd" )
        hive -e "$dws_complete_course_nd"
    ;;
    "dws_complete_user_chapter_nd" )
        hive -e "$dws_complete_user_chapter_nd"
    ;;
    "dws_course_review_info_nd" )
        hive -e "$dws_course_review_info_nd"
    ;;
    "dws_test_paper_exam_nd" )
        hive -e "$dws_test_paper_exam_nd"
    ;;
    "dws_test_answer_nd" )
        hive -e "$dws_test_answer_nd"
    ;;
    "all" )
        hive -e "$dws_trade_user_course_order_nd$dws_trade_province_order_nd$dws_consume_chapter_play_video_nd$dws_trade_user_order_nd$dws_complete_course_nd$dws_complete_user_chapter_nd$dws_course_review_info_nd$dws_test_paper_exam_nd$dws_test_answer_nd"
    ;;
esac

