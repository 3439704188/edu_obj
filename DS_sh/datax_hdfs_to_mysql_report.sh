#! /bin/bash

DATAX_HOME=/opt/module/datax

#DataX导出路径不允许存在空文件，该函数作用为清理空文件
handle_export_path(){
  for i in `hadoop fs -ls -R $1 | awk '{print $8}'`; do
    hadoop fs -test -z $i
    if [[ $? -eq 0 ]]; then
      echo "$i文件大小为0，正在删除"
      hadoop fs -rm -r -f $i
    fi
  done
}

#数据导出
export_data() {
  datax_config=$1
  export_dir=$2
  handle_export_path $export_dir
  $DATAX_HOME/bin/datax.py -p"-Dexportdir=$export_dir" $datax_config
}

case $1 in
  "ads_age_group_order_user_count")
    export_data /opt/module/datax/job/export/edu_report.ads_age_group_order_user_count.json /warehouse/edu/ads/ads_age_group_order_user_count
  ;;
  "ads_complete")
    export_data /opt/module/datax/job/export/edu_report.ads_complete.json /warehouse/edu/ads/ads_complete
  ;;
  "ads_complete_avg_chapter_by_course")
    export_data /opt/module/datax/job/export/edu_report.ads_complete_avg_chapter_by_course.json /warehouse/edu/ads/ads_complete_avg_chapter_by_course
  ;;
  "ads_complete_course")
    export_data /opt/module/datax/job/export/edu_report.ads_complete_course.json /warehouse/edu/ads/ads_complete_course
  ;;
  "ads_course_review_stats")
    export_data /opt/module/datax/job/export/edu_report.ads_course_review_stats.json /warehouse/edu/ads/ads_course_review_stats
  ;;
  "ads_course_video_play")
    export_data /opt/module/datax/job/export/edu_report.ads_course_video_play.json /warehouse/edu/ads/ads_course_video_play
  ;;
  "ads_new_order_user_stats")
    export_data /opt/module/datax/job/export/edu_report.ads_new_order_user_stats.json /warehouse/edu/ads/ads_new_order_user_stats
  ;;
  "ads_new_payment_user_stats")
    export_data /opt/module/datax/job/export/edu_report.ads_new_payment_user_stats.json /warehouse/edu/ads/ads_new_payment_user_stats
  ;;
  "ads_order")
    export_data /opt/module/datax/job/export/edu_report.ads_order.json /warehouse/edu/ads/ads_order
  ;;
  "ads_order_by_province")
    export_data /opt/module/datax/job/export/edu_report.ads_order_by_province.json /warehouse/edu/ads/ads_order_by_province
  ;;
  "ads_order_stats_by_category")
    export_data /opt/module/datax/job/export/edu_report.ads_order_stats_by_category.json /warehouse/edu/ads/ads_order_stats_by_category
  ;;
  "ads_order_stats_by_course")
    export_data /opt/module/datax/job/export/edu_report.ads_order_stats_by_course.json /warehouse/edu/ads/ads_order_stats_by_course
  ;;
  "ads_order_stats_by_subject")
    export_data /opt/module/datax/job/export/edu_report.ads_order_stats_by_subject.json /warehouse/edu/ads/ads_order_stats_by_subject
  ;;
  "ads_page_path")
    export_data /opt/module/datax/job/export/edu_report.ads_page_path.json /warehouse/edu/ads/ads_page_path
  ;;
  "ads_paper_test_stats")
    export_data /opt/module/datax/job/export/edu_report.ads_paper_test_stats.json /warehouse/edu/ads/ads_paper_test_stats
  ;;
  "ads_question_correct_rate_stats")
    export_data /opt/module/datax/job/export/edu_report.ads_question_correct_rate_stats.json /warehouse/edu/ads/ads_question_correct_rate_stats
  ;;
  "ads_shot_order_by_category")
    export_data /opt/module/datax/job/export/edu_report.ads_shot_order_by_category.json /warehouse/edu/ads/ads_shot_order_by_category
  ;;
  "ads_shot_order_by_course")
    export_data /opt/module/datax/job/export/edu_report.ads_shot_order_by_course.json /warehouse/edu/ads/ads_shot_order_by_course
  ;;
  "ads_shot_order_by_subject")
    export_data /opt/module/datax/job/export/edu_report.ads_shot_order_by_subject.json /warehouse/edu/ads/ads_shot_order_by_subject
  ;;
  "ads_traffic_order_by_source")
    export_data /opt/module/datax/job/export/edu_report.ads_traffic_order_by_source.json /warehouse/edu/ads/ads_traffic_order_by_source
  ;;
  "ads_traffic_stats_by_source")
    export_data /opt/module/datax/job/export/edu_report.ads_traffic_stats_by_source.json /warehouse/edu/ads/ads_traffic_stats_by_source
  ;;
  "ads_user_action")
    export_data /opt/module/datax/job/export/edu_report.ads_user_action.json /warehouse/edu/ads/ads_user_action
  ;;
  "ads_user_change")
    export_data /opt/module/datax/job/export/edu_report.ads_user_change.json /warehouse/edu/ads/ads_user_change
  ;;
  "ads_user_retention")
    export_data /opt/module/datax/job/export/edu_report.ads_user_retention.json /warehouse/edu/ads/ads_user_retention
  ;;
  "ads_user_stats")
    export_data /opt/module/datax/job/export/edu_report.ads_user_stats.json /warehouse/edu/ads/ads_user_stats
  ;;
  "ads_video_play")
    export_data /opt/module/datax/job/export/edu_report.ads_video_play.json /warehouse/edu/ads/ads_video_play
  ;;



  "all")
    export_data /opt/module/datax/job/export/edu_report.ads_age_group_order_user_count.json /warehouse/edu/ads/ads_age_group_order_user_count

    export_data /opt/module/datax/job/export/edu_report.ads_complete.json /warehouse/edu/ads/ads_complete

    export_data /opt/module/datax/job/export/edu_report.ads_complete_avg_chapter_by_course.json /warehouse/edu/ads/ads_complete_avg_chapter_by_course

    export_data /opt/module/datax/job/export/edu_report.ads_complete_course.json /warehouse/edu/ads/ads_complete_course

    export_data /opt/module/datax/job/export/edu_report.ads_course_review_stats.json /warehouse/edu/ads/ads_course_review_stats

    export_data /opt/module/datax/job/export/edu_report.ads_course_video_play.json /warehouse/edu/ads/ads_course_video_play

    export_data /opt/module/datax/job/export/edu_report.ads_new_order_user_stats.json /warehouse/edu/ads/ads_new_order_user_stats

    export_data /opt/module/datax/job/export/edu_report.ads_new_payment_user_stats.json /warehouse/edu/ads/ads_new_payment_user_stats

    export_data /opt/module/datax/job/export/edu_report.ads_order.json /warehouse/edu/ads/ads_order

    export_data /opt/module/datax/job/export/edu_report.ads_order_by_province.json /warehouse/edu/ads/ads_order_by_province

    export_data /opt/module/datax/job/export/edu_report.ads_order_stats_by_category.json /warehouse/edu/ads/ads_order_stats_by_category

    export_data /opt/module/datax/job/export/edu_report.ads_order_stats_by_course.json /warehouse/edu/ads/ads_order_stats_by_course

    export_data /opt/module/datax/job/export/edu_report.ads_order_stats_by_subject.json /warehouse/edu/ads/ads_order_stats_by_subject

    export_data /opt/module/datax/job/export/edu_report.ads_page_path.json /warehouse/edu/ads/ads_page_path

    export_data /opt/module/datax/job/export/edu_report.ads_paper_test_stats.json /warehouse/edu/ads/ads_paper_test_stats

    export_data /opt/module/datax/job/export/edu_report.ads_question_correct_rate_stats.json /warehouse/edu/ads/ads_question_correct_rate_stats

    export_data /opt/module/datax/job/export/edu_report.ads_shot_order_by_category.json /warehouse/edu/ads/ads_shot_order_by_category

    export_data /opt/module/datax/job/export/edu_report.ads_shot_order_by_course.json /warehouse/edu/ads/ads_shot_order_by_course

    export_data /opt/module/datax/job/export/edu_report.ads_shot_order_by_subject.json /warehouse/edu/ads/ads_shot_order_by_subject

    export_data /opt/module/datax/job/export/edu_report.ads_traffic_order_by_source.json /warehouse/edu/ads/ads_traffic_order_by_source

    export_data /opt/module/datax/job/export/edu_report.ads_traffic_stats_by_source.json /warehouse/edu/ads/ads_traffic_stats_by_source

    export_data /opt/module/datax/job/export/edu_report.ads_user_action.json /warehouse/edu/ads/ads_user_action

    export_data /opt/module/datax/job/export/edu_report.ads_user_change.json /warehouse/edu/ads/ads_user_change

    export_data /opt/module/datax/job/export/edu_report.ads_user_retention.json /warehouse/edu/ads/ads_user_retention

    export_data /opt/module/datax/job/export/edu_report.ads_user_stats.json /warehouse/edu/ads/ads_user_stats

    export_data /opt/module/datax/job/export/edu_report.ads_video_play.json /warehouse/edu/ads/ads_video_play

  ;;
esac
