#!/bin/bash

APP=edu

if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=`date -d "-1 day" +%F`
fi


dwd_trade_cart_full="
insert overwrite table ${APP}.dwd_trade_cart_full partition(dt='$do_date')
select
    id,
    user_id,
    course_id,
    course_name
from ${APP}.ods_cart_info_full
where dt='$do_date'
and sold='0';
"

dwd_consume_video_play_inc="
insert overwrite table ${APP}.dwd_consume_video_play_inc partition (dt='$do_date')
select distinct
    process.id         ,
    sub.category_id       ,
    course.subject_id ,
    process.course_id      ,
    process.chapter_id   ,
    video.id     ,
    process.user_id      ,
    process.position_sec      ,
    process.create_time      ,
    if(nvl(detail.create_time,'9999-12-31 00:00:00')>process.create_time,1,0)    ,
    process.deleted
from
(
    select
        id,
           course_id,
           chapter_id,
           user_id,
           position_sec,
           create_time,
           deleted
    from ${APP}.ods_user_chapter_process_full
    where dt = '$do_date'
) process
left join
(
    select
        id,
        chapter_id
    from ${APP}.ods_video_info_full
    where dt='$do_date'
) video
on process.chapter_id=video.chapter_id
left join
(
    select
        id,
        subject_id
    from ${APP}.ods_course_info_full
    where dt='$do_date'
) course
on course.id = process.course_id
left join
(
    select
        id,
        category_id
    from ${APP}.ods_base_subject_info_full
    where dt='$do_date'
) sub
on course.subject_id = sub.id
left join (
    select
        data.id,
        data.user_id
    from ${APP}.ods_order_info_inc
    where dt ='$do_date'
    and type='update'
    and data.order_status='1002'
) pay
on process.user_id = pay.user_id
left join (
    select
        data.course_id,
        data.create_time,
        data.order_id
    from ${APP}.ods_order_detail_inc
    where dt='$do_date'
    and type='insert'
) detail
on pay.id = detail.order_id;
"

dwd_consume_test_exam_inc="
insert overwrite table ${APP}.dwd_consume_test_exam_inc partition (dt='$do_date')
select
        exam.id,
        exam.paper_id,
        paper.course_id    ,
        category_id ,
        subject_id,
        exam.user_id,
        exam.score,
        exam.duration_sec,
        exam.create_time,
        exam.submit_time,
        exam.update_time,
        exam.deleted
from
(
    select
        data.id,
        data.paper_id,
        data.user_id,
        data.score,
        data.duration_sec,
        data.create_time,
        data.submit_time,
        data.update_time,
        data.deleted
    from ${APP}.ods_test_exam_inc
    where dt = '$do_date'
    and type='insert'
) exam
left join
(
    select
        id,
        course_id
    from ${APP}.ods_test_paper_full
    where dt='$do_date'
) paper
on exam.paper_id = paper.id
left join
(
    select
        id,
        subject_id
    from ${APP}.ods_course_info_full
    where dt='$do_date'
) course
on course.id = paper.course_id
left join
(
    select
        id,
        category_id
    from ${APP}.ods_base_subject_info_full
    where dt='$do_date'
) sub
on course.subject_id = sub.id;
"
dwd_user_register_inc="
insert overwrite table ${APP}.dwd_user_register_inc partition (dt='$do_date')
select
    distinct log.uid     ,
     users.date_id ,
     users.create_time,
    log.ch     ,
    log.ar   ,
    log.vc    ,
    log.mid    ,
    log.ba    ,
    log.md   ,
    log.os
from(
    select
        common.ar,
        common.uid,
        common.ch,
        common.vc,
        common.mid,
        common.ba,
        common.md,
        common.os
    from ${APP}.ods_log_inc
    where dt = '$do_date'
    and \`start\`.entry is not null
) log
left join
(
    select
        data.id,
        date_format(data.create_time,'yyyy-MM-dd') as date_id ,
        date_format(data.create_time,'HH-mm-ss') as create_time
    from ${APP}.ods_user_info_inc
    where dt='$do_date'
) users
on log.uid = users.id;
"


dwd_consume_test_question_inc="

insert overwrite table ${APP}.dwd_consume_test_question_inc partition (dt='$do_date')
select
    question.id ,
    category_id ,
    subject_id  ,
    course_id   ,
    chapter_id   ,
    exam_id ,
    paper_id ,
    question_id ,
    user_id ,
    answer ,
    is_correct ,
    score ,
    create_time,
    update_time,
    deleted 
from
(
    select
        data.id ,
        data.exam_id ,
        data.paper_id ,
        data.question_id ,
        data.user_id ,
        data.answer,
        data.is_correct,
        data.score,
        data.create_time ,
        data.update_time,
        data.deleted
    from ${APP}.ods_test_exam_question_inc
    where dt = '$do_date'
    and type='insert'
) question
left join
(
    select
        id,
        course_id
    from ${APP}.ods_test_paper_full
    where dt='$do_date'
) paper
on question.paper_id = paper.id
left join
(
    select
        id,
        subject_id
    from ${APP}.ods_course_info_full
    where dt='$do_date'
) course
on course.id = paper.course_id
left join
(
    select
        id,
        category_id
    from ${APP}.ods_base_subject_info_full
    where dt='$do_date'
) sub
on course.subject_id = sub.id
left join
(
    select
        id,
        chapter_id
    from ${APP}.ods_test_question_info_full
    where dt='$do_date'
) info
on question.question_id = info.id;
"


dwd_user_login_inc="
insert overwrite table ${APP}.dwd_user_login_inc partition (dt='$do_date')
select
     log.uid     ,
     log.date_id ,
     login_time,
    log.ch     ,
    log.ar   ,
    log.vc    ,
    log.mid    ,
    log.ba    ,
    log.md   ,
    log.os
from(
    select
        distinct common.sid,
        common.ar,
        common.uid,
        common.ch,
        common.vc,
        common.mid,
        common.ba,
        common.md,
        common.os,
        date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') as date_id,
        date_format(from_utc_timestamp(ts,'GMT+8'),'HH-mm-ss') as login_time
    from ${APP}.ods_log_inc
    where dt = '$do_date'
    and \`start\`.entry is not null
) log;
"


dwd_trade_cart_add_inc="
set hive.cbo.enable=false;

insert overwrite table ${APP}.dwd_trade_cart_add_inc partition (dt='$do_date')
select
    data.id                 ,
    data.user_id            ,
    data.course_id             ,
    date_format(data.create_time,'yyyy-MM-dd') date_id            ,
    data.create_time        ,
    data.cart_price            
from ${APP}.ods_cart_info_inc
where dt ='$do_date' and type = 'insert';
"

dwd_trade_order_detail_inc="
insert overwrite table ${APP}.dwd_trade_order_detail_inc partition (dt='$do_date')
select distinct
    od.id                    ,
    order_id              ,
    user_id               ,
    course_id                ,
    province_id           ,
    session_id            ,
    source_id            ,
    date_id               ,
    create_time           ,
    origin_amount ,
    coupon_reduce   ,
    final_amount    
from
(
select
    data.id                    ,
    data.order_id              ,
    data.course_id                ,
    date_format(data.create_time,'yyyy-MM-dd') date_id               ,
    data.create_time           ,
    data.origin_amount ,
    data.coupon_reduce   ,
    data.final_amount    
from ${APP}.ods_order_detail_inc
where dt = '$do_date' and type = 'insert'
) od
left join (
select
    data.id                    ,
    data.user_id               ,
    data.province_id           ,
    data.session_id            
from
${APP}.ods_order_info_inc
where dt = '$do_date' and type = 'insert'
)oi on od.order_id = oi.id
left join (
select
    common.sid           ,
    common.sc source_id 
from
${APP}.ods_log_inc
where dt = '$do_date'
)log on oi.session_id = log.sid;
"

dwd_trade_pay_detail_suc_inc="
insert overwrite table ${APP}.dwd_trade_pay_detail_suc_inc partition (dt='$do_date')
select
    od.id,
    od.order_id,
    user_id,
    course_id,
    province_id,
    payment_type,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    origin_amount,
    nvl(coupon_reduce,0.0),
    od.final_amount
from
(
select
        data.id,
        data.order_id,
        data.course_id,
        data.origin_amount,
        data.final_amount,
        data.coupon_reduce
from ${APP}.ods_order_detail_inc
where (dt = '$do_date' or dt = date_sub('$do_date',1))
and (type = 'insert' or type = 'bootstrap-insert')
) od
left join (
select
    data.id                    ,
    data.user_id               ,
    data.province_id
from
${APP}.ods_order_info_inc
where (dt = '$do_date' or dt = date_sub('$do_date',1)) and (type = 'insert' or type = 'bootstrap-insert')
)oi on od.order_id = oi.id
join (
select
    data.order_id             ,
    data.payment_type               ,
    data.callback_time
from
${APP}.ods_payment_info_inc
where dt = '$do_date'
and type = 'insert'
and data.payment_status='1602'
)pi on od.id = pi.order_id;
"

dwd_interaction_favor_add_inc="


insert overwrite table ${APP}.dwd_interaction_favor_add_inc partition(dt='$do_date')
select
    data.id,
    data.user_id,
    data.course_id,
    date_format(data.create_time,'yyyy-MM-dd') date_id,
    data.create_time
from ${APP}.ods_favor_info_inc
where dt='$do_date'
and type = 'insert';
"

dwd_interaction_comment_info_inc="


insert overwrite table ${APP}.dwd_interaction_comment_info_inc partition(dt='$do_date')
select
    data.id          ,
    data.user_id     ,
    data.chapter_id  ,
    data.course_id      ,
    data.comment_txt  ,
    date_format(data.create_time,'yyyy-MM-dd') date_id     ,
    data.create_time 
from ${APP}.ods_comment_info_inc
where dt='$do_date' and type = 'insert';
"


dwd_interaction_review_info_inc="


insert overwrite table ${APP}.dwd_interaction_review_info_inc partition(dt='$do_date')
select
    data.id          ,
    data.user_id     ,
    data.course_id      ,
    data.review_txt  ,
    data.review_stars  ,
    date_format(data.create_time,'yyyy-MM-dd') date_id     ,
    data.create_time 
from ${APP}.ods_review_info_inc
where dt='$do_date' and type = 'insert';
"


dwd_traffic_page_view_inc="

set hive.cbo.enable=false;
insert overwrite table ${APP}.dwd_traffic_page_view_inc partition (dt='$do_date')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    source_id,
    user_id,
    version_code,
    page_item,
    page_item_type,
    last_page_id,
    page_id,

    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') view_time,
    sid session_id,
    during_time
from
(
    select
        common.ar province_id,
        common.ba brand,
        common.ch channel,
        common.is_new is_new,
        common.md model,
        common.mid mid_id,
        common.os operate_system,
        common.sc source_id,
        common.uid user_id,
        common.vc version_code,
        common.sid sid,
        page.during_time,
        page.item page_item,
        page.item_type page_item_type,
        page.last_page_id,
        page.page_id,
        ts
    from ${APP}.ods_log_inc
    where dt='$do_date'
    and page is not null
)log;

"


dwd_traffic_actions_inc="
SET hive.support.quoted.identifiers=column;
set hive.cbo.enable=false;
insert overwrite table ${APP}.dwd_traffic_actions_inc partition (dt='$do_date')
select
    action_id    ,
    item        ,
    item_type    ,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id
from
(
    select
    struct1.action_id    ,
    struct1.item         ,
    struct1.item_type    ,
    struct1.ts
    from ${APP}.ods_log_inc
    lateral view explode(actions) array1 as struct1
    where dt='$do_date'
)log;

"


dwd_traffic_display_inc="
set hive.cbo.enable=false;
insert overwrite table ${APP}.dwd_traffic_display_inc partition (dt='$do_date')
select
    display_type   ,
    item           ,
    item_type      ,
    \`order\`          ,
    pos_id         
FROM
(
select
    struct1.display_type    ,
    struct1.item         ,
    struct1.item_type    ,
    struct1.\`order\`         ,
    struct1.pos_id
    from ${APP}.ods_log_inc
    lateral view explode(displays) array1 as struct1
    where dt='$do_date'
)log;
"


dwd_traffic_start_inc="

set hive.cbo.enable=false;
insert overwrite table ${APP}.dwd_traffic_start_inc partition (dt='$do_date')
select
    entry               ,
    first_open          ,
    loading_time        ,
    open_ad_id          ,
    open_ad_ms          ,
    open_ad_skip_ms     ,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd')            
from
(
select
    \`start\`.entry               ,
    \`start\`.first_open          ,
    \`start\`.loading_time        ,
    \`start\`.open_ad_id          ,
    \`start\`.open_ad_ms          ,
    \`start\`.open_ad_skip_ms     ,
    ts
    from ${APP}.ods_log_inc
    where dt='$do_date'
    and \`start\`.entry is not null
)log;

"

case $1 in
    "dwd_trade_cart_add_inc" )
        hive -e "$dwd_trade_cart_add_inc"
    ;;
    "dwd_trade_order_detail_inc" )
        hive -e "$dwd_trade_order_detail_inc"
    ;;
    "dwd_trade_pay_detail_suc_inc" )
        hive -e "$dwd_trade_pay_detail_suc_inc"
    ;;
    "dwd_trade_cart_full" )
        hive -e "$dwd_trade_cart_full"
    ;;
    "dwd_interaction_favor_add_inc" )
        hive -e "$dwd_interaction_favor_add_inc"
    ;;
    "dwd_interaction_comment_info_inc" )
        hive -e "$dwd_interaction_comment_info_inc"
    ;;
    "dwd_interaction_review_info_inc" )
        hive -e "$dwd_interaction_review_info_inc"
    ;;
    "dwd_traffic_page_view_inc" )
        hive -e "$dwd_traffic_page_view_inc"
    ;;
    "dwd_traffic_actions_inc" )
        hive -e "$dwd_traffic_actions_inc"
    ;;
    "dwd_traffic_display_inc" )
        hive -e "$dwd_traffic_display_inc"
    ;;
    "dwd_traffic_start_inc" )
        hive -e "$dwd_traffic_start_inc"
    ;;
    "dwd_consume_video_play_inc" )
        hive -e "$dwd_consume_video_play_inc"
    ;;
    "dwd_consume_test_exam_inc" )
        hive -e "$dwd_consume_test_exam_inc"
    ;;
    "dwd_consume_test_question_inc" )
        hive -e "$dwd_consume_test_question_inc"
    ;;
    "dwd_user_register_inc" )
        hive -e "$dwd_user_register_inc"
    ;;
    "dwd_user_login_inc" )
        hive -e "$dwd_user_login_inc"
    ;;
    "all" )
        hive -e "$dwd_traffic_display_inc$dwd_trade_cart_add_inc$dwd_trade_order_detail_inc$dwd_trade_pay_detail_suc_inc$dwd_trade_cart_full$dwd_interaction_favor_add_inc$dwd_interaction_comment_info_inc$dwd_interaction_review_info_inc$dwd_traffic_page_view_inc$dwd_traffic_actions_inc$dwd_traffic_start_inc$dwd_consume_video_play_inc$dwd_consume_test_exam_inc$dwd_consume_test_question_inc$dwd_user_register_inc$dwd_user_login_inc"
    ;;
esac
