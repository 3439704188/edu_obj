#! /bin/bash
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
"all")
  for item in /opt/module/datax/job/import/*.json
  do
    full=${item:33}
    dull=${full%.*}
    import_data $item /origin_data/edu/db/$dull"_full"/$do_date
  done
;;
*)
  echo --DIM--FULL--
  ;;
esac
