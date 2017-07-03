package com.haozhuo.bigdata.dataetl.labelgen.spark

import com.haozhuo.bigdata.dataetl.labelgen.spark.stream.ParseReport

/**
 * Created by hadoop on 6/7/17.
 */
object TestParseReport {
  def main(args: Array[String]) {
    //println(ParseAndSave.genIndexLabel("巨细胞病毒IGM抗体","阴性"))
    println(ParseReport.genSummaryLabel("眼科检查了常见症状，视力减退"))
  }
}
