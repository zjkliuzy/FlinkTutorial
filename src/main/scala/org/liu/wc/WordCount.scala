package org.liu.wc

import org.apache.flink.api.scala._

/**
 * 批处理
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建批处理执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //从文件中读取数据
    val inputDataSet: DataSet[String] = env.readTextFile("E:\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\hello.txt")
    //基于DataSet做统计,首先按空格分词打散，然后按照word作为key做group by
    val resultDataset: DataSet[(String, Int)] = inputDataSet
      .flatMap(_.split(" ")) //分词得到所有word构成的数据集
      .map((_, 1)) //抓换成1个2元组（word,count)
      .groupBy(0) //以2元组中第一个元素作为key分组
      .sum(1) //聚会元组中第二个元素的值
    resultDataset.print()

  }

}
