#!/bin/bash

DATAX_HOME=/opt/module/datax

# 如果传入日期则do_date等于传入的日期，否则等于前一天日期
if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=`date -d "-1 day" +%F`
fi

#处理目标路径，此处的处理逻辑是，如果目标路径不存在，则创建；若存在，则清空，目的是保证同步任务可重复执行
handle_targetdir() {
  hadoop fs -test -e $1
  if [[ $? -eq 1 ]]; then
    echo "路径$1不存在，正在创建......"
    hadoop fs -mkdir -p $1
  else
    echo "路径$1已经存在"
    
  fi
}

#数据同步
import_data() {
  datax_config=$1
  target_dir=$2

  handle_targetdir $target_dir
  python $DATAX_HOME/bin/datax.py -p"-Dtargetdir=$target_dir" $datax_config
}

case $1 in
"base_category_info")
  import_data /opt/module/datax/job/import/edu.base_category_info.json /origin_data/edu/db/base_category_info_full/$do_date
  ;;
"base_province")
  import_data /opt/module/datax/job/import/edu.base_province.json /origin_data/edu/db/base_province_full/$do_date
  ;;
"base_source")
  import_data /opt/module/datax/job/import/edu.base_source.json /origin_data/edu/db/base_source_full/$do_date
  ;;
"base_subject_info")
  import_data /opt/module/datax/job/import/edu.base_subject_info.json /origin_data/edu/db/base_subject_info_full/$do_date
  ;;
"cart_info")
  import_data /opt/module/datax/job/import/edu.cart_info.json /origin_data/edu/db/cart_info_full/$do_date
  ;;
"chapter_info")
  import_data /opt/module/datax/job/import/edu.chapter_info.json /origin_data/edu/db/chapter_info_full/$do_date
  ;;
"comment_info")
  import_data /opt/module/datax/job/import/edu.comment_info.json /origin_data/edu/db/comment_info_full/$do_date
  ;;
"course_info")
  import_data /opt/module/datax/job/import/edu.course_info.json /origin_data/edu/db/course_info_full/$do_date
  ;;
"favor_info")
  import_data /opt/module/datax/job/import/edu.favor_info.json /origin_data/edu/db/favor_info_full/$do_date
  ;;
"knowledge_point")
  import_data /opt/module/datax/job/import/edu.knowledge_point.json /origin_data/edu/db/knowledge_point_full/$do_date
  ;;
"order_detail")
  import_data /opt/module/datax/job/import/edu.order_detail.json /origin_data/edu/db/order_detail_full/$do_date
  ;;
"order_info")
  import_data /opt/module/datax/job/import/edu.order_info.json /origin_data/edu/db/order_info_full/$do_date
  ;;
"payment_info")
  import_data /opt/module/datax/job/import/edu.payment_info.json /origin_data/edu/db/payment_info_full/$do_date
  ;;
"review_info")
  import_data /opt/module/datax/job/import/edu.review_info.json /origin_data/edu/db/review_info_full/$do_date
  ;;
"test_exam")
  import_data /opt/module/datax/job/import/edu.test_exam.json /origin_data/edu/db/test_exam_full/$do_date
  ;;
"test_exam_question")
  import_data /opt/module/datax/job/import/edu.test_exam_question.json /origin_data/edu/db/test_exam_question_full/$do_date
  ;;
"test_paper")
  import_data /opt/module/datax/job/import/edu.test_paper.json /origin_data/edu/db/test_paper_full/$do_date
  ;;
"test_paper_question")
  import_data /opt/module/datax/job/import/edu.test_paper_question.json /origin_data/edu/db/test_paper_question_full/$do_date
  ;;
"test_point_question")
  import_data /opt/module/datax/job/import/edu.test_point_question.json /origin_data/edu/db/test_point_question_full/$do_date
  ;;
"test_question_info")
  import_data /opt/module/datax/job/import/edu.test_question_info.json /origin_data/edu/db/test_question_info_full/$do_date
  ;;
"test_question_option")
  import_data /opt/module/datax/job/import/edu.test_question_option.json /origin_data/edu/db/test_question_option_full/$do_date
  ;;
"user_chapter_process")
  import_data /opt/module/datax/job/import/edu.user_chapter_process.json /origin_data/edu/db/user_chapter_process_full/$do_date
  ;;
"user_info")
  import_data /opt/module/datax/job/import/edu.user_info.json /origin_data/edu/db/user_info_full/$do_date
  ;;
"video_info")
  import_data /opt/module/datax/job/import/edu.video_info.json /origin_data/edu/db/video_info_full/$do_date
  ;;
"vip_change_detail")
  import_data /opt/module/datax/job/import/edu.vip_change_detail.json /origin_data/edu/db/vip_change_detail_full/$do_date
  ;;
"all")
  import_data /opt/module/datax/job/import/edu.base_category_info.json /origin_data/edu/db/base_category_info_full/$do_date
  import_data /opt/module/datax/job/import/edu.base_province.json /origin_data/edu/db/base_province_full/$do_date
  import_data /opt/module/datax/job/import/edu.base_source.json /origin_data/edu/db/base_source_full/$do_date
  import_data /opt/module/datax/job/import/edu.base_subject_info.json /origin_data/edu/db/base_subject_info_full/$do_date
  import_data /opt/module/datax/job/import/edu.cart_info.json /origin_data/edu/db/cart_info_full/$do_date
  import_data /opt/module/datax/job/import/edu.chapter_info.json /origin_data/edu/db/chapter_info_full/$do_date
  import_data /opt/module/datax/job/import/edu.comment_info.json /origin_data/edu/db/comment_info_full/$do_date
  import_data /opt/module/datax/job/import/edu.course_info.json /origin_data/edu/db/course_info_full/$do_date
  import_data /opt/module/datax/job/import/edu.favor_info.json /origin_data/edu/db/favor_info_full/$do_date
  import_data /opt/module/datax/job/import/edu.knowledge_point.json /origin_data/edu/db/knowledge_point_full/$do_date
  import_data /opt/module/datax/job/import/edu.order_detail.json /origin_data/edu/db/order_detail_full/$do_date
  import_data /opt/module/datax/job/import/edu.order_info.json /origin_data/edu/db/order_info_full/$do_date
  import_data /opt/module/datax/job/import/edu.payment_info.json /origin_data/edu/db/payment_info_full/$do_date
  import_data /opt/module/datax/job/import/edu.review_info.json /origin_data/edu/db/review_info_full/$do_date
  import_data /opt/module/datax/job/import/edu.test_exam.json /origin_data/edu/db/test_exam_full/$do_date
  import_data /opt/module/datax/job/import/edu.test_exam_question.json /origin_data/edu/db/test_exam_question_full/$do_date
  import_data /opt/module/datax/job/import/edu.test_paper.json /origin_data/edu/db/test_paper_full/$do_date
  import_data /opt/module/datax/job/import/edu.test_paper_question.json /origin_data/edu/db/test_paper_question_full/$do_date
  import_data /opt/module/datax/job/import/edu.test_point_question.json /origin_data/edu/db/test_point_question_full/$do_date
  import_data /opt/module/datax/job/import/edu.test_question_info.json /origin_data/edu/db/test_question_info_full/$do_date
  import_data /opt/module/datax/job/import/edu.test_question_option.json /origin_data/edu/db/test_question_option_full/$do_date
  import_data /opt/module/datax/job/import/edu.user_chapter_process.json /origin_data/edu/db/user_chapter_process_full/$do_date
  import_data /opt/module/datax/job/import/edu.user_info.json /origin_data/edu/db/user_info_full/$do_date
  import_data /opt/module/datax/job/import/edu.video_info.json /origin_data/edu/db/video_info_full/$do_date
  import_data /opt/module/datax/job/import/edu.vip_change_detail.json /origin_data/edu/db/vip_change_detail_full/$do_date
  ;;
esac

