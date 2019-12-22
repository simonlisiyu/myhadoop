package com.lsy.myhadoop.hive.sql

class HiveSqlSentence {
  val cityId = 12

  /**
    * sql
    */
  val selectNormal =
    s"""
         SELECT
       |   a.id,
       |   a.in_id
       | FROM
       |   tmp_${cityId} a
       | """.stripMargin

  val selectJoin =
    s"""
           SELECT
       |   a.city_id,
       |   b.sum,
       |   round(a.one_cum / b.all_sum, 4) as one_percent
       | FROM
       |   tmp1 a
       | JOIN
       |   tmp2 b
       | ON
       |   a.date = b.date""".stripMargin

  val selectLeftOuterJoin =
    s"""
           SELECT
       |   a.start_id,
       |   a.last_id,
       |   a.end__id,
       |   a.city_id
       |FROM
       |   tmp1 a
       |LEFT OUTER JOIN
       |   (
       |    SELECT * FROM
       |      tmp2
       |    WHERE
       |       city_id = '${cityId}'
       |   ) b
       |ON(
       |   a.start_id = b.start_id
       |   AND a.last_id = b.last_id
       |  )
        """.stripMargin

  val selectGroupBy = s"""
                        | SELECT
                        |    first(a.city_id) as city_id,
                        |    a.id,
                        |    a.in_id,
                        |    a.out_id,
                        |    count(*) as one_sum,
                        |    round(avg(time), 4) as avg_time
                        | FROM
                        |    test_table a
                        | WHERE
                        |    a.city_id = ${cityId} AND
                        | GROUP BY
                        |  a.id,
                        |  a.in_id,
                        |  a.out_id
                        | """.stripMargin

  // WHERE关键字在使用集合函数时不能使用，所以在集合函数中加上了HAVING来起到测试查询结果是否符合条件的作用。
  val selectGroupByHaving = s"""
                         | SELECT
                         |    first(a.city_id) as city_id,
                         |    a.id,
                         |    a.in_id,
                         |    a.out_id,
                         |    count(*) as one_sum,
                         |    round(avg(time), 4) as avg_time
                         | FROM
                         |    test_table a
                         | WHERE
                         |    a.city_id = ${cityId} AND
                         | GROUP BY
                         |    a.id,
                         |    a.in_id,
                         |    a.out_id
                         | HAVING
                         |    avg_time <= 25 AND avg_time >=6
                         | """.stripMargin

  val selectFromLateralViewExpolde = s"""
                                        | SELECT
                                        |    a.city_id,
                                        |    a.id,
                                        |    a.in_id,
                                        |    a.start_time,
                                        |    a.end_time,
                                        |    a.out_id
                                        | FROM
                                        |    test_table a
                                        |     LATERAL VIEW explode(id) id as t
                                        | WHERE
                                        |    a.city_id = ${cityId} AND
                                        |    size(a.id) > 3 AND
                                        |    t.average_speed < 0.667
                                        | """.stripMargin

  /**
    * compute
    * Hive常用函数大全一览
    * https://www.iteblog.com/archives/2258.html
    */
  val count_word = "count(*) as cell_sum"
  val sum_word = "sum(a.cell_sum) as time_slice_sum"
  val max_word = "max(a.time_slice_saturation_degree) as movement_saturation_degree"
  val substr_word = "substr(a.id, 1, 2) as id_prefix"
  val concat_ws_word = "concat_ws(\",\",cast(linkid as string),cast(cityid as string)) as c"  //指定分隔符，链接字段
  val floor_word = "floor(double a)" // 对给定数据进行向下舍入最接近的整数

  // if(boolean testCondition, T valueTrue, T valueFalseOrNull)
  // 当条件testCondition为TRUE时，返回valueTrue；否则返回valueFalseOrNull
  val if_word = "if(1=2,100,200)"

  // 指定精度取整函数: 返回指定精度d的double类型，没有d则取整，返回double类型的整数值部分（遵循四舍五入）
  val round_word = "round(a.cell_sum / b.time_slice_sum, 4) as cell_percent"

  // 非空查找函数: 返回参数中的第一个非空值；如果所有值都为NULL，那么返回NULL
  val coalesce_word = "COALESCE(b.speed, 15) as freeSpeed"

  // 近似中位数函数 percentile_approx(DOUBLE col,  p [, B])
  // 近似中位数函数 percentile_approx(DOUBLE col, array(p1 [, p2]…) [, B])
  // 求近似的第pth个百分位数，p必须介于0和1之间，返回类型为double，
  // 但是col字段支持浮点类型。参数B控制内存消耗的近似精度，B越大，结果的准确度越高。
  // 默认为10,000。当col字段中的distinct值的个数小于B时，结果为准确的百分位数
  // Array功能和上述类似，之后后面可以输入多个百分位数，返回类型也为array<double>，其中为对应的百分位数。
  // 返回值: double or array<double>
  val percentile_approx_word = "percentile_approx(a.speed, 0.5) as free_speed"


  val case_word = "CASE WHEN a.cell_delay > a.red_duration THEN a.cell_delay / a.red_duration * a.cell_percent"+
    "WHEN a.cell_delay <= a.red_duration AND a.cell_delay > 0.3 THEN 1 * a.cell_percent"+
    "ELSE round(1 * a.cell_sum / b.movement_cell_max * a.cell_percent, 4)"+
    "END AS cell_saturation_degree"

  /**
    * Hive分析窗口函数(一) SUM,AVG,MIN,MAX
    * http://lxw1234.com/archives/2015/04/176.htm
    * http://lxw1234.com/search/Hive%E5%88%86%E6%9E%90%E7%AA%97%E5%8F%A3
    *
    * 如果不指定ROWS BETWEEN,默认为从起点到当前行;
      如果不指定ORDER BY，则将分组内所有值累加;
      关键是理解ROWS BETWEEN含义,也叫做WINDOW子句：
      PRECEDING：往前
      FOLLOWING：往后
      CURRENT ROW：当前行
      UNBOUNDED：起点，
      UNBOUNDED PRECEDING 表示从前面的起点，
      UNBOUNDED FOLLOWING：表示到后面的终点
    */
  // 默认为从起点到当前行
  val sum_window_word1 = "SUM(pv) OVER(PARTITION BY cookieid ORDER BY createtime) AS pv1"
  // 从起点到当前行，结果同pv1
  val sum_window_word2 = "SUM(pv) OVER(PARTITION BY cookieid ORDER BY createtime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pv2"
  // 分组内所有行
  val sum_window_word3 = "SUM(pv) OVER(PARTITION BY cookieid) AS pv3"
  // 当前行+往前3行
  val sum_window_word4 = "SUM(pv) OVER(PARTITION BY cookieid ORDER BY createtime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS pv4"
  // 当前行+往前3行+往后1行
  val sum_window_word5 = "SUM(pv) OVER(PARTITION BY cookieid ORDER BY createtime ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING) AS pv5"
  // 当前行+往后所有行
  val sum_window_word6 = "SUM(pv) OVER(PARTITION BY cookieid ORDER BY createtime ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS pv6 "

}
