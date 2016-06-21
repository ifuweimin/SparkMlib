package rec

import java.net.URLDecoder

import com.alibaba.fastjson.JSON
import com.test.CnWords
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fuweimin on 2016/6/20.
  */
object Relatedsw {

  def analysis(url: String, key: String): String = {
    if(!url.isEmpty){
      val s = url.substring(url.indexOf('?') + 1)
      val paramaters = s.split("&")
      for (param <- paramaters) {
        val values = param.split("=")
        if (values(0) == key) {
          if (values.length != 1) {
            return values(1)
          }
        }
      }
    }
    ""
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("cf user-based").setMaster("local")
    val sc = new SparkContext(sparkConf)
    //hdfs://master:9000/data/*/{*.1464505211742,*.1464508811425}
    val lines = sc.wholeTextFiles("hdfs://master:9000/data/logs/*").values
      .flatMap(t => t.split("\n|\r|\r\n"))

    val data = lines.map { line =>
      try {
        var q = ""
        val jsonobj = JSON.parseObject("{" + line + "}")
        val request_path = jsonobj.getString("request_path")
        if(jsonobj.containsKey("http_referer")) {
          val http_referer = jsonobj.getString("http_referer")
          q = analysis(http_referer, "sw") //检索词
        }
        if(q.isEmpty) {
          ("", "")
        } else {
          (URLDecoder.decode(q, "UTF-8"), request_path)
        }
      }catch {
        case ex: Exception => ("", "")
      }
    }.filter(line => !line._1.isEmpty && !line._2.isEmpty && line._2.indexOf("detail_") != -1)

    val count = data.map(f => ((f._1 + "@#" + f._2), 1)).reduceByKey(_+_).map(f => (f._1.split("@#")(1), f._1.split("@#")(0), math.sqrt(f._2)))

    val userdata = count.map(f =>
      (ItemPref(f._1, f._2, f._3))
    ).cache()

    //2 建立模型
    val mysimil = new ItemSimilarity()
    val simil_rdd1 = mysimil.Similarity(userdata, "cosine")

//    println(s"物品相似度矩阵: ${simil_rdd1.count()}")
//    simil_rdd1.collect().foreach { ItemSimi =>
//      println(ItemSimi.itemid1 + ", " + ItemSimi.itemid2 + ", " + ItemSimi.similar)
//    }

    //排序截取
    val result = simil_rdd1.filter(f => !CnWords.parse(f.itemid1).equals(CnWords.parse(f.itemid2))).map(f => ((f.itemid1), (f.itemid2, f.similar))).groupByKey()
    val limit_number = 20 //取结果
    val final_result  = result.map(f => {
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortWith(_._2 > _._2)
      if (i2_2.length > limit_number) i2_2.remove(0, (i2_2.length - limit_number))
      (f._1, i2_2.toIterable)
    })

    //打印
    final_result.foreach(f => {
      val key = f._1
      val values_iter = f._2.iterator
      var string_values = ""
      while (values_iter.hasNext) {
        val tmpvalue = values_iter.next()
        println(key+"   "+tmpvalue)
      }
    })

    //入hbase
//    final_result.foreach(f => {
//      val key = URLEncoder.encode(f._1,"UTF-8")
//      val values_iter = f._2.iterator
//      var string_values = ""
//      while (values_iter.hasNext) {
//        val tmpvalue = values_iter.next()
//        if(string_values.isEmpty)
//          string_values += "{\"w\":\""+URLEncoder.encode(tmpvalue._1,"UTF-8")+"\",\"s\":\""+tmpvalue._2+"\"}"
//        else
//          string_values += ",{\"w\":\""+URLEncoder.encode(tmpvalue._1,"UTF-8")+"\",\"s\":\""+tmpvalue._2+"\"}"
//      }
//      HbaseUtils.insertData(key,string_values)
//    })


    /**

    final_result.foreachPartition{
      x=> {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "web102,web101,web100")
        myConf.set("hbase.zookeeper.property.clientPort", "2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, TableName.valueOf(tableName))
        myTable.setAutoFlush(false, false)//关键点1
        myTable.setWriteBufferSize(3*1024*1024)//关键点2
        x.foreach { y => {
          println(y(0) + ":::" + y(1))
          val p = new Put(Bytes.toBytes(y(0)))
          p.add("Family".getBytes, "qualifier".getBytes, Bytes.toBytes(y(1)))
          myTable.put(p)
        }
        }
        myTable.flushCommits()//关键点3
      }
      **/

  }
}
