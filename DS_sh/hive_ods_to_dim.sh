#!/bin/bash

APP=edu

if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=`date -d "-1 day" +%F`
fi

dim_user_zip="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dim_user_zip partition (dt)
select
    id           ,
    real_name    ,
    phone_num    ,
    email        ,
    user_level   ,
    birthday     ,
    gender       ,
    create_time  ,
    operate_time ,
    start_date   ,
    if ( rn = 1, '9999-12-31',date_sub('$do_date', 1) ) end_date ,    
    if ( rn = 1, '9999-12-31',date_sub('$do_date', 1) ) dt
from (
    select
        *,
        row_number() over ( partition by id order by start_date desc) rn
    from (
        select
            id           ,
            real_name    ,
            phone_num    ,
            email        ,
            user_level   ,
            birthday     ,
            gender       ,
            create_time  ,
            operate_time ,
            start_date   ,
            end_date
        from ${APP}.dim_user_zip
        where dt = '9999-12-31'
        union all
        select
            data.id           ,
            concat(substr(data.real_name, 1, 1), '*')       real_name,
            if(data.phone_num regexp'^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
            concat(substr(data.phone_num, 1, 3), '*'), null) phone_num,
            if(data.email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',
            concat('*@', split(data.email, '@')[1]), null)   email,
            data.user_level   ,
            data.birthday     ,
            data.gender       ,
            data.create_time  ,
            data.operate_time ,
            '$do_date'   ,
            '9999-12-31'
        from (
            select
                *,
                row_number() over (partition by data.id order by ts desc) num
            from ${APP}.ods_user_info_inc
            where dt = '$do_date'
       ) t where num = 1
    ) t
) t1;
"


dim_knowledge_full="
set hive.exec.dynamic.partition.mode=nonstrict;
with
kn as (
select
    id,
    point_txt ,
    point_level ,
    course_id  ,
    chapter_id ,
    create_time ,
    update_time ,
    publisher_id ,
    deleted 
from ${APP}.ods_knowledge_point_full
where dt = '$do_date'
),
ch as (
select
    id           ,
    chapter_name  
from ${APP}.ods_chapter_info_full
where dt = '$do_date'
),
co as (
select
    id           ,
    course_name  
from ${APP}.ods_course_info_full
where dt = '$do_date'
)
insert overwrite table ${APP}.dim_knowledge_full partition(dt='$do_date')
select
    kn.id  ,
    point_txt ,
    point_level,
    course_id  ,
    course_name  ,
    chapter_id ,
    chapter_name ,
    create_time ,
    update_time  ,
    publisher_id  ,
    deleted    
from kn
left join ch on kn.chapter_id = ch.id
left join co on kn.course_id = co.id;
"

dim_question_info_full="
set hive.exec.dynamic.partition.mode=nonstrict;
with
qu as (
select
    id             ,
    question_txt   ,
    chapter_id     ,
    course_id      ,
    question_type  ,
    create_time    ,
    update_time    ,
    publisher_id   ,
    deleted        
from ${APP}.ods_test_question_info_full
where dt = '$do_date' and chapter_id = '1'
),
ch as (
select
    id           ,
    chapter_name  
from ${APP}.ods_chapter_info_full
where dt = '$do_date'
),
co as (
select
    id           ,
    course_name  
from ${APP}.ods_course_info_full
where dt = '$do_date'
),
qo as (
select
    question_id ,
    collect_list(named_struct('option_id', id, 'option_txt', option_txt,'is_correct', is_correct)) question_option
from ${APP}.ods_test_question_option_full
where dt = '$do_date'
group by question_id
)
insert overwrite table ${APP}.dim_question_info_full partition(dt='$do_date')
select
    qu.id          ,
    question_txt   ,
    chapter_id     ,
    chapter_name   ,
    course_id      ,
    course_name    ,
    question_type  ,
    create_time    ,
    update_time    ,
    publisher_id   ,
    deleted        ,
    qo.question_option
from qu
left join ch on qu.chapter_id = ch.id
left join co on qu.course_id = co.id
left join qo on qu.id = qo.question_id;
"

dim_video_full="
set hive.exec.dynamic.partition.mode=nonstrict;
with video1 as(
select
    id,
    video_name,
    during_sec,
    video_status,
    video_size,
    video_url,
    video_source_id,
    version_id,
    chapter_id,
    course_id,
    publisher_id,
    create_time,
    update_time,
    deleted
from ${APP}.ods_video_info_full
where dt = '$do_date'
),
course as(
select
id , course_name
from ${APP}.ods_course_info_full
where dt = '$do_date'),
chapter1 as(
select
id , chapter_name
from ${APP}.ods_chapter_info_full
where dt = '$do_date')
insert overwrite table ${APP}.dim_video_full partition(dt = '$do_date')
select
    video1.id,
    video1.video_name,
    video1.during_sec,
    video1.video_status,
    video1.video_size ,
    video1.video_url ,
    video1.video_source_id ,
    video1.version_id,
    video1.chapter_id,
    chapter1.chapter_name,
    video1.course_id,
    course.course_name,
    video1.publisher_id,
    video1.create_time ,
    video1.update_time ,
    deleted
from video1
left join course on
video1.course_id=course.id
left join chapter1 on
 video1.chapter_id=chapter1.id;
"

dim_chapter_full="
set hive.exec.dynamic.partition.mode=nonstrict;
with chapter as(
select
    id,
    chapter_name,
    course_id,
    video_id ,
    publisher_id ,
    is_free ,
    create_time ,
    deleted ,
    update_time
from ${APP}.ods_chapter_info_full
where dt = '$do_date'
),
course as(
select
id , course_name
from ${APP}.ods_course_info_full
where dt = '$do_date'),
video as(
select
id , video_name
from ${APP}.ods_video_info_full
where dt = '$do_date')
insert overwrite table ${APP}.dim_chapter_full partition(dt = '$do_date')
select
    chapter.id ,
    chapter.chapter_name,
    chapter.course_id,
    course_name,
    chapter.video_id ,
    video_name,
    chapter.publisher_id,
    chapter.is_free,
    chapter.create_time,
    chapter.deleted ,
    chapter.update_time
from chapter
left join course on
course.id=chapter.course_id
left join video on
chapter.video_id = video.id;
"

dim_course_full="
set hive.exec.dynamic.partition.mode=nonstrict;
with course as(
select
    id ,
    course_name ,
    course_slogan ,
    course_cover_url ,
    subject_id ,
    teacher ,
    publisher_id ,
    chapter_num ,
    origin_price ,
    reduce_amount ,
    actual_price ,
    course_introduce ,
    create_time,
    deleted,
    update_time
from ${APP}.ods_course_info_full
where dt = '$do_date'
),
subject as(
select
id , subject_name, category_id
from ${APP}.ods_base_subject_info_full
where dt = '$do_date'),
category as(
select
id , category_name
from ${APP}.ods_base_category_info_full
where dt = '$do_date')
insert overwrite table ${APP}.dim_course_full partition(dt = '$do_date')
select
    course.id,
    course.course_name,
    course.course_slogan, 
    course.course_cover_url, 
    course.subject_id, 
    subject.subject_name, 
    subject.category_id, 
    category.category_name, 
    course.teacher, 
    course.publisher_id, 
    course.chapter_num, 
    course.origin_price, 
    course.reduce_amount, 
    course.actual_price, 
    course.course_introduce, 
    course.create_time, 
    course.deleted, 
    course.update_time 
from course
left join subject on
course.subject_id=subject.id
left join category on
subject.category_id = category.id;
"

dim_province_full="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dim_province_full partition(dt='$do_date')
select
    id          ,
    name ,
    region_id     ,
    area_code     ,
    iso_code      ,
    iso_3166_2    
from ${APP}.ods_base_province_full
where dt='$do_date';"


dim_source_full="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dim_source_full partition(dt='$do_date')
select
    id,
    source_site,
    source_url
from ${APP}.ods_base_source_full
where dt='$do_date';"

dim_paper_full="
set hive.exec.dynamic.partition.mode=nonstrict;
with
p as (
select
    id            ,
    paper_title   ,
    course_id     ,
    create_time   ,
    update_time   ,
    publisher_id  ,
    deleted       
from ${APP}.ods_test_paper_full
where dt = '$do_date'
),
c as (
select
    id           ,
    course_name  
from ${APP}.ods_course_info_full
where dt = '$do_date'
)
insert overwrite table ${APP}.dim_paper_full partition(dt='$do_date')
select
    p.id          ,
    paper_title   ,
    course_id     ,
    course_name   ,
    create_time   ,
    update_time   ,
    publisher_id  ,
    deleted       
from p
left join c on p.course_id = c.id;"



case $1 in
"dim_user_zip")
    hive -e "$dim_user_zip"
;;
"dim_video_full")
    hive -e "$dim_video_full"
;;
"dim_province_full")
    hive -e "$dim_province_full"
;;
"dim_source_full")
    hive -e "$dim_source_full"
;;
"dim_course_full")
    hive -e "$dim_course_full"
;;
"dim_chapter_full")
    hive -e "$dim_chapter_full"
;;
"dim_knowledge_full")
    hive -e "$dim_knowledge_full"
;;
"dim_paper_full")
    hive -e "$dim_paper_full"
;;
"dim_question_info_full")
    hive -e "$dim_question_info_full"
;;
"all")
    hive -e "$dim_user_zip$dim_video_full$dim_province_full$dim_source_full$dim_course_full$dim_chapter_full$dim_knowledge_full$dim_paper_full$dim_question_info_full"
;;
esac
