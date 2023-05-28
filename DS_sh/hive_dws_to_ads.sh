#!/bin/bash

APP=edu

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=`date -d "-1 day" +%F`
fi


ads_traffic_stats_by_source="
insert overwrite table ${APP}.ads_traffic_stats_by_source
select * from ${APP}.ads_traffic_stats_by_source
union(
select
       data.dt,
           recent_days,
    source_id,source_name,uv_count,avg_duration_sec,avg_page_count,sv_count,bounce_rate
        from
(select
    '$do_date' dt,
    recent_days,
    source_id,
    cast(count(distinct(mid_id)) as bigint) uv_count,
    cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,
    cast(avg(page_count_1d) as bigint) avg_page_count,
    cast(count(*) as bigint) sv_count,
    cast(sum(if(page_count_1d=1,1,0))/count(*) as decimal(16,2)) bounce_rate
from ${APP}.dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt>=date_add('$do_date',-recent_days+1)
group by recent_days,source_id) data
left join ${APP}.dim_source_full dm
on dm.id=data.source_id);
"


ads_page_path="
insert overwrite table ${APP}.ads_page_path
select * from ${APP}.ads_page_path
union(
select
    '$do_date' dt,
    recent_days,
    source,
    nvl(target,'null'),
    count(*) path_count
from
(
    select
           dt,
        concat('step-',rn,':',page_id) source,
        concat('step-',rn+1,':',next_page_id) target
    from
    (
        select
            dt,
            page_id,
            lead(page_id,1,null) over(partition by session_id order by view_time) next_page_id,
            row_number() over (partition by session_id order by view_time) rn
        from ${APP}.dwd_traffic_page_view_inc
    )t1
)t2
lateral view explode(array(1,7,30)) tmp as recent_days
        where dt>=date_add('$do_date',-recent_days+1)
        group by recent_days,source,target);"

ads_traffic_order_by_source="
set hive.exec.dynamic.partition.mode=nonstrict;

set hive.vectorized.execution.enabled = false;
insert overwrite table ${APP}.ads_traffic_order_by_source
select *
from ${APP}.ads_traffic_order_by_source
union(
select
       '$do_date' dt,
       cast(recent_days as bigint) rtd,
       data.source_id,
       source_name,
       cast (total_amount as decimal(16,2)) tm,
       cast (buyer/all_view as decimal(16,2)) rate
from (
    select
             recent_days,
             source_id,
             sum(final_amount) as    total_amount,
             count(distinct user_id) buyer
      from ${APP}.dwd_trade_order_detail_inc
               lateral view explode(array(1, 7, 30)) tmp as recent_days
      where dt >= date_add('$do_date', -recent_days + 1)
    group by recent_days, source_id, final_amount
      ) data
         left join ${APP}.dim_source_full dm
                   on dm.id = data.source_id
right join
    (select source_id,
            count(distinct user_id) as all_view from ${APP}.dwd_traffic_page_view_inc group by source_id) dd
on data.source_id=dd.source_id);"


ads_order_by_province="
insert overwrite table ${APP}.ads_order_by_province
select * from ${APP}.ads_order_by_province
union(
select
    dt,
    recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2 ,
    order_count,
    order_user_count,
    order_total_amount
from
(
select
    '$do_date' dt,
    1 recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2 ,
    sum(order_count_1d) order_count,
    sum(order_user_count_1d) order_user_count,
    sum(order_total_amount_1d) order_total_amount
from ${APP}.dws_trade_province_order_1d
where dt='$do_date'
group by province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2
union all
select
    dt,
    recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2 ,
    sum(order_count) order_count,
    sum(order_user_count) order_user_count,
    sum(order_total_amount) order_total_amount
from(
select
    '$do_date' dt,
    recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    case recent_days
        when 7 then order_count_7d
        when 30 then order_count_30d
    end order_count,
    case recent_days
        when 7 then order_user_count_7d
        when 30 then order_user_count_30d
    end order_user_count,
    case recent_days
        when 7 then order_total_amount_7d
        when 30 then order_total_amount_30d
    end order_total_amount
from ${APP}.dws_trade_province_order_nd lateral view explode(array(7,30)) tmp as recent_days
where dt='$do_date') t
group by dt,recent_days,
         province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2) t1);"


ads_order="
insert overwrite table ${APP}.ads_order
select * from ${APP}.ads_order
union(
select
    '$do_date' dt,
    1 recent_days,
    sum(order_count_1d),
    count(distinct user_id),
    sum(order_total_amount_1d)
from ${APP}.dws_trade_user_order_1d
where dt='$do_date'
union all
select
    '$do_date' dt,
    recent_days,
    case recent_days
        when 7 then sum(order_count_7d)
        when 30 then sum(order_count_30d)
    end order_count,
    case recent_days
        when 7 then count(distinct if(order_count_7d>0,user_id,null))
        when 30 then count(distinct user_id)
    end order_user_count,
    case recent_days
        when 7 then sum(order_total_amount_7d)
        when 30 then sum(order_total_amount_30d)
    end order_total_amount
from ${APP}.dws_trade_user_order_nd lateral view explode(array(7,30)) tmp as recent_days
where dt='$do_date'
group by tmp.recent_days);"

ads_video_play="
insert overwrite table ${APP}.ads_video_play
select * from ${APP}.ads_video_play
union(
select
    1 recent_days,
    cast(chapter.id as string),
    chapter_name,
    nvl(view_count_1d,0),
    nvl(position_sec_1d/user_count_1d,0),
    nvl(user_count_1d,0),
    '$do_date' dt
from (
    select id,
           chapter_name
    from
    ${APP}.dim_chapter_full
where dt='$do_date'
    ) chapter
left join
(select
        chapter_id,
        view_count_1d,
        user_count_1d,
        position_sec_1d
        from
    ${APP}.dws_consume_chapter_play_video_1d
    where dt='$do_date'
    ) video
on chapter.id=video.chapter_id
union all
select
    recent_days,
    cast(chapter.id as string),
    chapter_name,
    nvl(view_count,0),
    nvl(pos_sec/user_count,0),
    nvl(user_count,0),
    '$do_date' dt
from (
    select id,
           chapter_name
    from
    ${APP}.dim_chapter_full
where dt='$do_date'
    ) chapter
left join
(select
    chapter_id,
    recent_days,
    case recent_days
        when 7 then view_count_7d
        when 30 then view_count_30d
    end view_count,
    case recent_days
        when 7 then position_sec_7d
        when 30 then position_sec_30d
    end pos_sec,
    case recent_days
        when 7 then user_count_7d
        when 30 then user_count_30d
    end user_count,
    '$do_date' dt
from ${APP}.dws_consume_chapter_play_video_nd lateral view explode(array(7,30)) tmp as recent_days
where dt='$do_date'
) vid2
on chapter.id=vid2.chapter_id);"


ads_course_video_play="
insert overwrite table ${APP}.ads_course_video_play
select * from ${APP}.ads_course_video_play
union(
select
    1 recent_days,
    cast(course.id as string),
    course_name,
    nvl(view_count_1d,0),
    nvl(position_sec_1d/user_count_1d,0),
    nvl(user_count_1d,0),
    '$do_date' dt
from (
    select id,
           course_name
    from
    ${APP}.dim_course_full
where dt='$do_date'
    ) course
left join
(select
        course_id,
        view_count_1d,
        user_count_1d,
        position_sec_1d
        from
    ${APP}.dws_consume_chapter_play_video_1d
    where dt='$do_date'
    ) video
on course.id=video.course_id
union all
select
    recent_days,
    cast(course.id as string),
    course_name,
    nvl(view_count,0),
    nvl(pos_sec/user_count,0),
    nvl(user_count,0),
    '$do_date' dt
from (
    select id,
           course_name
    from
    ${APP}.dim_course_full
where dt='$do_date'
    ) course
left join
(select
    course_id,
    recent_days,
    case recent_days
        when 7 then view_count_7d
        when 30 then view_count_30d
    end view_count,
    case recent_days
        when 7 then position_sec_7d
        when 30 then position_sec_30d
    end pos_sec,
    case recent_days
        when 7 then user_count_7d
        when 30 then user_count_30d
    end user_count,
    '$do_date' dt
from ${APP}.dws_consume_chapter_play_video_nd lateral view explode(array(7,30)) tmp as recent_days
where dt='$do_date'
) vid2
on course.id=vid2.course_id);"


ads_complete_course="
insert overwrite table ${APP}.ads_complete_course
select * from ${APP}.ads_complete_course
union(
select
    1 ,
    course.id,
    course_name,
    sum(nvl(complete_user_count_1d,0)),
    '$do_date'
from (
    select id,
           course_name
        from
    ${APP}.dim_course_full
    where dt='$do_date'
    ) course
left join
(select
        course_id,
        complete_user_count_1d
        from
    ${APP}.dws_complete_course_1d
    where dt='$do_date'
    ) video
on course.id=video.course_id
group by course.id, course_name
union all
select
       recent_days,
       course_id,
       course_name,
        sum(complete_user_count),
       dt
from (
         select recent_days,
                course.id course_id,
                course_name,
                cast(nvl(case recent_days
                             when 7 then complete_user_count_7d
                             when 30 then complete_user_count_30d
                             end, 0) as bigint) complete_user_count,
                '$do_date'                    dt
         from (
                  select id,
                         course_name,
                         tmp.recent_days

                  from ${APP}.dim_course_full
                           lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '$do_date'
              ) course
                  left join
              (select course_id,
                      complete_user_count_7d,
                      complete_user_count_30d,
                      '$do_date' dt
               from ${APP}.dws_complete_course_nd
               where dt = '$do_date'
              ) vid2
              on course.id = vid2.course_id
     ) t1
    group by recent_days,
            course_id,
            course_name,
            dt
);"

ads_complete="
insert overwrite table ${APP}.ads_complete
select * from ${APP}.ads_complete
union (select
    1 ,
    d1,
    dc1,
    '$do_date'
from
(select
        nvl(sum(complete_user_count_1d),0) d1,
        nvl(sum(complete_count_1d),0) dc1
        from
    ${APP}.dws_complete_course_1d
    where dt='$do_date'
    ) video
union all
select
    recent_days,sum(complete_user_count),sum(complete_count),dt
from(
select
    recent_days,
    cast(case recent_days
        when 7 then d7
        when 30 then d30
    end  as bigint) complete_user_count,
    cast(case recent_days
        when 7 then dc7
        when 30 then dc30
    end  as bigint) complete_count,
    '$do_date' dt
from
(select
    nvl(sum(complete_user_count_7d),0) d7,
    nvl(sum(complete_user_count_30d),0) d30,
    nvl(sum(complete_count_7d),0) dc7,
    nvl(sum(complete_count_30d),0) dc30,
    '$do_date' dt
from ${APP}.dws_complete_course_nd
    where dt='$do_date'
) vid2
lateral view explode(array(7,30)) tmp as recent_days)t
    group by recent_days,dt
);"


ads_complete_avg_chapter_by_course="
insert overwrite table ${APP}.ads_complete_avg_chapter_by_course
select * from ${APP}.ads_complete_avg_chapter_by_course
union(
select 1,
       hi.course_id,
       course_name,
       rat,
       '$do_date'
from (
         select 1,
                course_id,
                count(1) / count(distinct user_id) rat,
                '$do_date'
         from ${APP}.dws_complete_user_chapter_first_date_1d
         where dt = '$do_date'
    group by course_id) hi
left join(
    select *
        from
    ${APP}.dim_course_full
           where dt='$do_date') sc
on sc.id=hi.course_id
union all
select recent_days,
            course_id,
       course_name,
       if(recent_days=7,d7,d30)/users,
       '$do_date'
from (
select
       recent_days,
       course_id,
       course_name,
       sum(complete_7d) d7,
       sum(complete_30d) d30,
       count(distinct user_id) users
from ${APP}.dws_complete_user_chapter_nd
lateral view explode(array(7,30)) tmp as recent_days
where dt='$do_date'
group by course_id,course_name,recent_days) nd);"

ads_user_change="
insert overwrite table ${APP}.ads_user_change
select * from ${APP}.ads_user_change
union(
select
    churn.dt,
    user_churn_count,
    user_back_count
from
(
    select
        '$do_date' dt,
        count(*) user_churn_count
    from ${APP}.dws_user_user_login_td
    where dt='$do_date'
    and login_date_last=date_add('$do_date',-7)
)churn
join
(
    select
        '$do_date' dt,
        count(*) user_back_count
    from
    (
        select
            user_id,
            login_date_last
        from ${APP}.dws_user_user_login_td
        where dt='$do_date'
    )t1
    join
    (
        select
            user_id,
            login_date_last login_date_previous
        from ${APP}.dws_user_user_login_td
        where dt=date_add('$do_date',-1)
    )t2
    on t1.user_id=t2.user_id
    where datediff(login_date_last,login_date_previous)>=8
)back
on churn.dt=back.dt);"


ads_user_retention="
insert overwrite table ${APP}.ads_user_retention
select * from ${APP}.ads_user_retention
union(
select '$do_date' dt,
       login_date_first create_date,
       datediff('$do_date', login_date_first) retention_day,
       sum(if(login_date_last = '$do_date', 1, 0)) retention_count,
       count(*) new_user_count,
       cast(sum(if(login_date_last = '$do_date', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (
         select user_id,
                login_date_last,
                login_date_first
         from ${APP}.dws_user_user_login_td
         where dt = '$do_date'
           and login_date_first >= date_add('$do_date', -7)
           and login_date_first < '$do_date'
     ) t1
group by login_date_first);"

ads_user_stats="
insert overwrite table ${APP}.ads_user_stats
select * from ${APP}.ads_user_stats
union(
select '$do_date' dt,
       recent_days,
       sum(if(login_date_first >= date_add('$do_date', -recent_days + 1), 1, 0)) new_user_count,
       count(*) active_user_count
from ${APP}.dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt = '$do_date'
  and login_date_last >= date_add('$do_date', -recent_days + 1)
group by recent_days);"


ads_user_action="
insert overwrite table ${APP}.ads_user_action
select * from ${APP}.ads_user_action
union(
select
    '$do_date' dt,
    home_count,
    good_detail_count,
    cart_count,
    order_count,
    payment_count
from
(
    select
        1 recent_days,
        sum(if(page_id='home',1,0)) home_count,
        sum(if(page_id='good_detail',1,0)) good_detail_count
    from ${APP}.dws_traffic_page_visitor_page_view_1d
    where dt='$do_date'
    and page_id in ('home','good_detail')
)page
join
(
    select
        1 recent_days,
        count(*) cart_count
    from ${APP}.dws_trade_user_cart_add_1d
    where dt='$do_date'
)cart
on page.recent_days=cart.recent_days
join
(
    select
        1 recent_days,
        count(*) order_count
    from ${APP}.dws_trade_user_order_1d
    where dt='$do_date'
)ord
on page.recent_days=ord.recent_days
join
(
    select
        1 recent_days,
        count(*) payment_count
    from ${APP}.dws_trade_user_payment_1d
    where dt='$do_date'
)pay
on page.recent_days=pay.recent_days);"


ads_new_order_user_stats="
insert overwrite table ${APP}.ads_new_order_user_stats
select * from ${APP}.ads_new_order_user_stats
union(
select
    '$do_date' dt,
    recent_days,
    count(*) new_order_user_count
from ${APP}.dws_trade_user_order_td lateral view explode(array(1,7,30)) tmp as recent_days
where dt='$do_date'
and order_date_first>=date_add('$do_date',-recent_days+1)
group by recent_days);"


ads_new_payment_user_stats="
insert overwrite table ${APP}.ads_new_payment_user_stats
select * from ${APP}.ads_new_payment_user_stats
union(
select
    '$do_date' dt,
    recent_days,
    count(*) new_payment_user_count
from ${APP}.dws_trade_user_payment_td lateral view explode(array(1,7,30)) tmp as recent_days
where dt='$do_date'
and payment_date_first>=date_add('$do_date',-recent_days+1)
group by recent_days);"


ads_age_group_order_user_count="
insert overwrite table ${APP}.ads_age_group_order_user_count
select *
from ${APP}.ads_age_group_order_user_count
union(
select '$do_date' dt,
       recent_days,
       age_group,
       count(distinct (user_id))
from (
         select dt,
                recent_days,
                user_id
         from ${APP}.dws_trade_user_order_td lateral view explode(array(1, 7, 30)) tmp as recent_days
     ) o
         join (select id,
                      case
                          when age > 0 and age <= 6 then '0-6岁'
                          when age > 6 and age <= 12 then '7-12岁'
                          when age > 12 and age <= 17 then '13-17岁'
                          when age > 17 and age <= 45 then '18-45岁'
                          when age > 45 and age <= 69 then '46-69岁'
                          else '69岁以上'
                          end age_group
               from (
                        select id,
                               FLOOR(datediff('$do_date', birthday) / 365.25) as age
                        from ${APP}.dim_user_zip
                    ) t) u on o.user_id = u.id
where dt = '$do_date'
group by recent_days, age_group);"


ads_order_stats_by_category="
insert overwrite table ${APP}.ads_order_stats_by_category
select * from ${APP}.ads_order_stats_by_category
union(
select
    '$do_date' dt,
    recent_days,
    category_id,
    category_name,
    order_count,
    order_user_count,
    order_amount_sum
from
(
    select
        1 recent_days,
        category_id,
        category_name,
        sum(order_count_1d) order_count,
        count(distinct(user_id)) order_user_count,
        sum(order_total_amount_1d) order_amount_sum
    from ${APP}.dws_trade_user_course_order_1d
    where dt='$do_date'
    group by category_id,category_name
    union all
    select
        recent_days,
        category_id,
        category_name,
        sum(order_count),
        count(distinct(if(order_count>0,user_id,null))),
        sum(order_total_amount_nd) order_amount_sum
    from
    (
        select
            recent_days,
            user_id,
            category_id,
            category_name,
            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count,
            case recent_days
                when 7 then order_total_amount_7d
                when 30 then order_total_amount_30d
            end order_total_amount_nd
        from ${APP}.dws_trade_user_course_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='$do_date'
    )t1
    group by recent_days,category_id,category_name
)odr);"



ads_order_stats_by_subject="
insert overwrite table ${APP}.ads_order_stats_by_subject
select * from ${APP}.ads_order_stats_by_subject
union(
select
    '$do_date' dt,
    recent_days,

    subject_id,
    subject_name,

    order_count,
    order_user_count,
    order_amount_sum
from
(
    select
        1 recent_days,
        subject_id,
        subject_name,
        sum(order_count_1d) order_count,
        count(distinct(user_id)) order_user_count,
        sum(order_total_amount_1d) order_amount_sum
    from ${APP}.dws_trade_user_course_order_1d
    where dt='$do_date'
    group by subject_id,subject_name
    union all
    select
        recent_days,
        subject_id,
        subject_name,
        sum(order_count),
        count(distinct(if(order_count>0,user_id,null))),
        sum(order_total_amount_nd) order_count
    from
    (
        select
            recent_days,
            user_id,

            subject_id,
            subject_name,

            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count,
            case recent_days
                when 7 then order_total_amount_7d
                when 30 then order_total_amount_30d
            end order_total_amount_nd
        from ${APP}.dws_trade_user_course_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='$do_date'
    )t1
    group by recent_days,subject_id,subject_name
)odr);"


ads_order_stats_by_course="
insert overwrite table ${APP}.ads_order_stats_by_course
select * from ${APP}.ads_order_stats_by_course
union(
select
    '$do_date' dt,
    recent_days,
    course_id,
    course_name,
    order_count,
    order_user_count,
    order_amount_sum
from
(
    select
        1 recent_days,
        course_id,
        course_name,
        sum(order_count_1d) order_count,
        count(distinct(user_id)) order_user_count,
        sum(order_total_amount_1d) order_amount_sum
    from ${APP}.dws_trade_user_course_order_1d
    where dt='$do_date'
    group by course_id,course_name,subject_id,subject_name,category_id,category_name
    union all
    select
        recent_days,
        course_id,
        course_name,
        sum(order_count),
        count(distinct(if(order_count>0,user_id,null))),
        sum(order_total_amount_nd)
    from
    (
        select
            recent_days,
            user_id,
            course_id,
            course_name,
            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count,
            case recent_days
                when 7 then order_total_amount_7d
                when 30 then order_total_amount_30d
            end order_total_amount_nd
        from ${APP}.dws_trade_user_course_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='$do_date'
    )t1
    group by recent_days,course_id,course_name
)odr);"


ads_course_review_stats="
insert overwrite table ${APP}.ads_course_review_stats
select * from ${APP}.ads_course_review_stats
union(
select
       '$do_date' dt,
       recent_days,
       course_id,
       course_name,
       avg_user_review,
       user_count,
       favorable_rate
from
(
    select
        1 recent_days,
        course_id,
        course_name,
        avg(review_sum) avg_user_review,
        sum(user_count) user_count,
       sum(user_count_5)/sum(user_count) favorable_rate
    from ${APP}.dws_course_review_info_1d
    where dt='$do_date'
    group by course_id,course_name
    union all
    select
        recent_days,
        course_id,
        course_name,
        avg(review_sum) avg_user_review,
        sum(user_count) user_count,
       sum(user_count_5)/sum(user_count) favorable_rate
    from
    (
        select
            recent_days,
            course_id,
            course_name,
            case recent_days
                when 7 then review_sum_7d
                when 30 then review_sum_30d
            end review_sum,
            case recent_days
                when 7 then user_count_7d
                when 30 then user_count_30d
            end user_count,
            case recent_days
                when 7 then user_count_5_7d
                when 30 then user_count_5_30d
            end user_count_5
        from ${APP}.dws_course_review_info_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='$do_date'
    )t1
    group by recent_days,course_id,course_name
)odr);"

ads_paper_test_stats="
insert overwrite table ${APP}.ads_paper_test_stats
select * from ${APP}.ads_paper_test_stats
union(
select
    '$do_date' dt,
    recent_days,
    course_id,
    avg_score,
    avg_dur_time,
    user_count
from
(
    select
        1 recent_days,
    course_id,
        sum(score_sum_1d)/sum(user_count_1d) avg_score,
        sum(during_time_1d)/sum(user_count_1d) avg_dur_time,
        sum(user_count_1d)  user_count
    from ${APP}.dws_test_paper_exam_1d
    where dt='$do_date'
    group by course_id
    union all
    select
        recent_days,
        course_id,
        sum(score_sum_nd)/sum(user_count_nd) avg_score,
        sum(during_time_nd)/sum(user_count_nd) avg_dur_time,
        sum(user_count_nd)  user_count
    from
    (
        select
            recent_days,
            course_id,
            case recent_days
                when 7 then score_sum_7d
                when 30 then score_sum_30d
            end score_sum_nd,
            case recent_days
                when 7 then during_time_7d
                when 30 then during_time_30d
            end during_time_nd,
            case recent_days
                when 7 then user_count_7d
                when 30 then user_count_30d
            end user_count_nd
        from ${APP}.dws_test_paper_exam_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='$do_date'
    )t1
    group by recent_days,course_id
)odr);"


ads_question_correct_rate_stats="
insert overwrite table ${APP}.ads_question_correct_rate_stats
select * from ${APP}.ads_question_correct_rate_stats
union(
select
    '$do_date' dt,
    recent_days,
    question_id,
    correct_rate
from
(
    select
        1 recent_days,
        question_id,
        sum(answer_correct_count_1d)/sum(answer_count_1d) correct_rate
    from ${APP}.dws_test_answer_1d
    where dt='$do_date'
    group by question_id
    union all
    select
        recent_days,
        question_id,
        sum(answer_correct_count_nd)/sum(answer_count_nd) correct_rate
    from
    (
        select
            recent_days,
            question_id,
            case recent_days
                when 7 then answer_count_7d
                when 30 then answer_count_30d
            end answer_count_nd,
            case recent_days
                when 7 then answer_correct_count_7d
                when 30 then answer_correct_count_30d
            end answer_correct_count_nd
        from ${APP}.dws_test_answer_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='$do_date'
    )t1
    group by recent_days,question_id
)odr);"



ads_shot_order_by_category="
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled = false;
insert overwrite table ${APP}.ads_shot_order_by_category
select * from ${APP}.ads_shot_order_by_category
union(
select
        '$do_date' dt,
        1 recent_days,
        cast(vp.category_id as bigint as bigint) category_id,
        '编程技术' category_name,
       count(DISTINCT vp.user_id) cp,
       count(DISTINCT if(uco.user_id=vp.user_id,uco.user_id,null))/count(DISTINCT vp.user_id) rat
from (SELECT
             dt,
                    1 category_id,
                    1 recent_days,
                    user_id
         from ${APP}.dwd_trade_order_detail_inc
         where dt='$do_date'
     ) UCO
         RIGHT join
     (
         select
                    category_id,
                     user_id
         from ${APP}.dwd_consume_video_play_inc
         where dt='$do_date' and is_shot = 1
     ) vp on uco.category_id = vp.category_id
     where vp.category_id=1
group by vp.category_id
union all
select '$do_date' dt,
        recent_days,
        uco.category_id,
        '编程技术' category_name,
       if(recent_days=7,count(DISTINCT if(dt>=date_add('$do_date',-6),vp.user_id,NULL)),count(DISTINCT vp.user_id)),
       if(recent_days=7,count(DISTINCT if(dt>=date_add('$do_date',-6) and uco.user_id=vp.user_id,uco.user_id,NULL))/count(DISTINCT if(dt>=date_add('$do_date',-6),vp.user_id,NULL)),count(DISTINCT if(uco.user_id=vp.user_id,uco.user_id,NULL))/count(DISTINCT vp.user_id))
from (SELECT
             dt,
                    1 category_id,
                    TMP.recent_days,
                    user_id
         from ${APP}.dwd_trade_order_detail_inc
LATERAL VIEW EXPLODE(array(7,30))  TMP AS recent_days
         where dt>=date_add('$do_date',-29) and dt<='$do_date'
     ) UCO
         RIGHT join
     (
         select
                    category_id,
                    user_id
         from ${APP}.dwd_consume_video_play_inc
         where dt>=date_add('$do_date',-29) and dt<='$do_date' and is_shot = 1

     ) vp on uco.category_id = vp.category_id
where vp.category_id=1
group by recent_days,UCO.category_id);"



ads_shot_order_by_subject="
insert overwrite table ${APP}.ads_shot_order_by_subject
select * from ${APP}.ads_shot_order_by_subject
union(
    select
        '$do_date' dt,
        1 recent_days,
        cast(vp.subject_id as bigint) subject_id,
        subject_name,
       count(distinct vp.user_id),
       count(DISTINCT if(vm.user_id=vp.user_id,vm.user_id,null))/count(distinct vp.user_id) rat
from (
    select
        subject_name,
           subject_id,
           user_id
        from
    (
    SELECT
            course_id,
            1 recent_days,
            user_id
         from ${APP}.dwd_trade_order_detail_inc
             where dt='$do_date') dic
        left join (select id,subject_id,subject_name
            from ${APP}.dim_course_full
            where dt='$do_date') dm on dic.course_id=dm.id
    ) vm

         RIGHT join
     (
         select
                    subject_id,
                    user_id
         from ${APP}.dwd_consume_video_play_inc
         where dt='$do_date' and is_shot = 1
     ) vp on vm.subject_id = vp.subject_id
group by vp.subject_id,subject_name
union all
select
       '$do_date' dt,
        recent_days,
        vt.subject_id,
        subject_name,
       if(recent_days=7,count(distinct if(dt>=date_add('$do_date',-6),vp.user_id,NULL)),count(distinct vp.user_id)),
       if(recent_days=7,count(distinct if(dt>=date_add('$do_date',-6) and vt.user_id=vp.user_id,vt.user_id,NULL))/count(distinct if(dt>=date_add('$do_date',-6),vp.user_id,NULL)),count(distinct if(vt.user_id=vp.user_id,vt.user_id,null))/count(distinct vp.user_id))
from (select
             dt,
             subject_name,
             subject_id,
             recent_days,
             user_id
             from
        (SELECT
                     dt,
                    course_id,
                    recent_days,
                    user_id
         from ${APP}.dwd_trade_order_detail_inc
        LATERAL VIEW EXPLODE(array(7,30))  TMP AS recent_days
         where dt>=date_add('$do_date',-29) and dt<='$do_date'
     ) UCO
    full join (select id,
                      subject_id,
                      subject_name
            from ${APP}.dim_course_full
            where dt='$do_date') dm on uco.course_id=dm.id
    ) vt
         RIGHT join
     (
         select
                subject_id,
                  user_id
         from ${APP}.dwd_consume_video_play_inc
         where dt>=date_add('$do_date',-29) and dt<='$do_date' and is_shot = 1
     ) vp on vt.subject_id = vp.subject_id
group by recent_days,vt.subject_id,vt.subject_name
);"

ads_shot_order_by_course="
insert overwrite table ${APP}.ads_shot_order_by_course
select * from ${APP}.ads_shot_order_by_course
union(select
        '$do_date' dt,
        1 recent_days,
        cast(vp.course_id as bigint) course_id,
        course_name,
       count(distinct vp.user_id) shot,
       count(distinct if(vm.user_id=vp.user_id,vm.user_id,null))/count(distinct vp.user_id) rat
from (
    select
        course_name,
           course_id,
        user_id
        from
    (
    SELECT
            course_id,
            1 recent_days,
            user_id
         from ${APP}.dwd_trade_order_detail_inc
             where dt='$do_date'
        ) dic
        left join (select id,course_name
            from ${APP}.dim_course_full
            where dt='$do_date') dm on dic.course_id=dm.id
    ) vm
         right join
     (
         select
                    course_id,
                    user_id
         from ${APP}.dwd_consume_video_play_inc
         where dt='$do_date' and is_shot = 1
     ) vp on vm.course_id = vp.course_id
group by vp.course_id,course_name
union all
select
        '$do_date' dt,
        recent_days,
        cast(vp.course_id as bigint) course_id,
        course_name,
       if(recent_days=7,count(distinct if(dt>=date_add('$do_date',-6),vp.user_id,null)) ,count(distinct vp.user_id) ) shot,
       if(recent_days=7,count(distinct if(dt>=date_add('$do_date',-6) and vm.user_id=vp.user_id,vm.user_id,null))/count(distinct if(dt>=date_add('$do_date',-6),vp.user_id,null)),count(distinct if(vm.user_id=vp.user_id, vm.user_id,null))/count(distinct vp.user_id)) rat
from (
    select
        dt,
        course_name,
        recent_days,
        course_id,
        user_id
        from
    (
    SELECT
           dt,
            course_id,
            recent_days,
            user_id
         from ${APP}.dwd_trade_order_detail_inc
       LATERAL VIEW EXPLODE(array(7,30))  TMP AS recent_days
             where dt>=date_add('$do_date',-29) and dt<='$do_date'
        ) dic
        left join (select id,course_name
            from ${APP}.dim_course_full
            where dt='$do_date') dm on dic.course_id=dm.id
    ) vm
        full join
     (
         select
                    course_id,
                    user_id
         from ${APP}.dwd_consume_video_play_inc
         where dt>=date_add('$do_date',-29) and dt<='$do_date'
           and is_shot = 1
     ) vp on vm.course_id = vp.course_id
group by vp.course_id,course_name,recent_days
);"










case $1 in
"ads_age_group_order_user_count" )
        hive -e "$ads_age_group_order_user_count"
    ;;

"ads_complete" )
        hive -e "$ads_complete"
    ;;

"ads_complete_avg_chapter_by_course" )
        hive -e "$ads_complete_avg_chapter_by_course"
    ;;

"ads_complete_course" )
        hive -e "$ads_complete_course"
    ;;

"ads_course_review_stats" )
        hive -e "$ads_course_review_stats"
    ;;

"ads_course_video_play" )
        hive -e "$ads_course_video_play"
    ;;

"ads_new_order_user_stats" )
        hive -e "$ads_new_order_user_stats"
    ;;

"ads_new_payment_user_stats" )
        hive -e "$ads_new_payment_user_stats"
    ;;

"ads_order" )
        hive -e "$ads_order"
    ;;

"ads_order_by_province" )
        hive -e "$ads_order_by_province"
    ;;

"ads_order_stats_by_category" )
        hive -e "$ads_order_stats_by_category"
    ;;

"ads_order_stats_by_course" )
        hive -e "$ads_order_stats_by_course"
    ;;

"ads_order_stats_by_subject" )
        hive -e "$ads_order_stats_by_subject"
    ;;

"ads_page_path" )
        hive -e "$ads_page_path"
    ;;

"ads_paper_test_stats" )
        hive -e "$ads_paper_test_stats"
    ;;

"ads_question_correct_rate_stats" )
        hive -e "$ads_question_correct_rate_stats"
    ;;

"ads_shot_order_by_category" )
        hive -e "$ads_shot_order_by_category"
    ;;

"ads_shot_order_by_course" )
        hive -e "$ads_shot_order_by_course"
    ;;

"ads_shot_order_by_subject" )
        hive -e "$ads_shot_order_by_subject"
    ;;

"ads_traffic_order_by_source" )
        hive -e "$ads_traffic_order_by_source"
    ;;

"ads_traffic_stats_by_source" )
        hive -e "$ads_traffic_stats_by_source"
    ;;

"ads_user_action" )
        hive -e "$ads_user_action"
    ;;

"ads_user_change" )
        hive -e "$ads_user_change"
    ;;

"ads_user_retention" )
        hive -e "$ads_user_retention"
    ;;

"ads_user_stats" )
        hive -e "$ads_user_stats"
    ;;

"ads_video_play" )
        hive -e "$ads_video_play"
    ;;

    "all" )
        hive -e "$ads_age_group_order_user_count
$ads_complete
$ads_complete_avg_chapter_by_course
$ads_complete_course
$ads_course_review_stats
$ads_course_video_play
$ads_new_order_user_stats
$ads_new_payment_user_stats
$ads_order
$ads_order_by_province
$ads_order_stats_by_category
$ads_order_stats_by_course
$ads_order_stats_by_subject
$ads_page_path
$ads_paper_test_stats
$ads_question_correct_rate_stats
$ads_shot_order_by_category
$ads_shot_order_by_course
$ads_shot_order_by_subject
$ads_traffic_order_by_source
$ads_traffic_stats_by_source
$ads_user_action
$ads_user_change
$ads_user_retention
$ads_user_stats
$ads_video_play"
    ;;
esac
