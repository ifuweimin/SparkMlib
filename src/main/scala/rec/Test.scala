package rec

/**
  * Created by fuweimin on 2016/6/15.
  */

import java.net.URLEncoder

import com.test.HbaseUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ItemCF {

  //def sortByValue(v1:Float,v2:Float):

  def main(args: Array[String]) {

    //0 构建Spark对象
    val conf = new SparkConf().setAppName("ItemCF").setMaster("local[2]")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.INFO)

    //1 读取样本数据
    val data_path = "hdfs://master:9000/data/q2url"
    val data = sc.textFile(data_path)

    val prodata = data.map(line => {
      val vs = line.split(";#")
      try {
        (RecUtils.get_detail(vs(1)), vs(0), vs(2).toDouble)
      }catch {
        case ex: Exception => ("","", math.sqrt(0.toDouble))
      }
    }).filter(f => !f._1.isEmpty)

    val userdata = prodata.map(f =>
      (ItemPref(f._1, f._2, f._3))
    ).cache()

    //2 建立模型
    val mysimil = new ItemSimilarity()
    val simil_rdd1 = mysimil.Similarity(userdata, "cosine")
//    val recommd = new RecommendedItem
//    val recommd_rdd1 = recommd.Recommend(simil_rdd1, userdata, 10)

    //3 打印结果
    println(s"物品相似度矩阵: ${simil_rdd1.count()}")

    val result = simil_rdd1.map(f => ((f.itemid1), (f.itemid2, f.similar))).groupByKey()
    val r_number = 2

    val final_result  = result.map(f => {
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortWith(_._2 > _._2)
      if (i2_2.length > r_number) i2_2.remove(0, (i2_2.length - r_number))
      (f._1, i2_2.toIterable)
    })

//    val final_result = result.flatMap{
//      case(a,b) =>
//        val topk=b.toArray.sortWith{ (a,b) => a._2 > b._2 }.take(3)
//        topk.zipWithIndex
//    }
    final_result.foreach(f => {
      val key = URLEncoder.encode(f._1,"UTF-8")
      val values_iter = f._2.iterator
      var string_values = ""
       while (values_iter.hasNext) {
         val tmpvalue = values_iter.next()
         if(string_values.isEmpty)
           string_values += "{\"w\":\""+URLEncoder.encode(tmpvalue._1,"UTF-8")+"\",\"s\":\""+tmpvalue._2+"\"}"
         else
           string_values += ",{\"w\":\""+URLEncoder.encode(tmpvalue._1,"UTF-8")+"\",\"s\":\""+tmpvalue._2+"\"}"
       }
      HbaseUtils.insertData(key,string_values)
    })

//    simil_rdd1.collect().foreach { ItemSimi =>
//        println(ItemSimi.itemid1 + ", " + ItemSimi.itemid2 + ", " + ItemSimi.similar)
//    }

//    println(s"用戶推荐列表: ${recommd_rdd1.count()}")
//    recommd_rdd1.collect().foreach { UserRecomm =>
//      println(UserRecomm.userid + ", " + UserRecomm.itemid + ", " + UserRecomm.pref)
//    }

  }
}