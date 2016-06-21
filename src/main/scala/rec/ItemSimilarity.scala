package rec

/**
  * Created by fuweimin on 2016/6/15.
  */

import org.apache.spark.rdd.RDD

import scala.math._

/**
  * 用户评分.
  *
  * @param userid 用户
  * @param itemid 评分物品
  * @param pref 评分
  */
case class ItemPref(
                     val userid: String,
                     val itemid: String,
                     val pref: Double) extends Serializable
/**
  * 用户推荐.
  *
  * @param userid 用户
  * @param itemid 推荐物品
  * @param pref 评分
  */
case class UserRecomm(
                       val userid: String,
                       val itemid: String,
                       val pref: Double) extends Serializable
/**
  * 相似度.
  *
  * @param itemid1 物品
  * @param itemid2 物品
  * @param similar 相似度
  */
case class ItemSimi(
                     val itemid1: String,
                     val itemid2: String,
                     val similar: Double) extends Serializable

/**
  * 相似度计算.
  * 支持：同现相似度、欧氏距离相似度、余弦相似度
  *
  */
class ItemSimilarity extends Serializable {

  /**
    * 相似度计算.
    *
    * @param user_rdd 用户评分
    * @param stype 计算相似度公式
    * @param RDD[ItemSimi] 返回物品相似度
    *
    */
  def Similarity(user_rdd: RDD[ItemPref], stype: String): (RDD[ItemSimi]) = {
    val simil_rdd = stype match {
      case "cooccurrence" =>
        ItemSimilarity.CooccurrenceSimilarity(user_rdd)
      case "cosine" =>
        ItemSimilarity.CosineSimilarity(user_rdd)
      case "euclidean" =>
        ItemSimilarity.EuclideanDistanceSimilarity(user_rdd)
      case _ =>
        ItemSimilarity.CooccurrenceSimilarity(user_rdd)
    }
    simil_rdd
  }

}

object ItemSimilarity {

  /**
    * 同现相似度矩阵计算.
    * w(i,j) = N(i)∩N(j)/sqrt(N(i)*N(j))
    *
    * @param user_rdd 用户评分
    * @param RDD[ItemSimi] 返回物品相似度
    *
    */
  def CooccurrenceSimilarity(user_rdd: RDD[ItemPref]): (RDD[ItemSimi]) = {
    // 0 数据做准备
    val user_rdd1 = user_rdd.map(f => (f.userid, f.itemid, f.pref))
    val user_rdd2 = user_rdd1.map(f => (f._1, f._2))
    // 1 (用户：物品) 笛卡尔积 (用户：物品) => 物品:物品组合
    val user_rdd3 = user_rdd2.join(user_rdd2)
    val user_rdd4 = user_rdd3.map(f => (f._2, 1))
    // 2 物品:物品:频次
    val user_rdd5 = user_rdd4.reduceByKey((x, y) => x + y)
    // 3 对角矩阵
    val user_rdd6 = user_rdd5.filter(f => f._1._1 == f._1._2)
    // 4 非对角矩阵
    val user_rdd7 = user_rdd5.filter(f => f._1._1 != f._1._2)
    // 5 计算同现相似度（物品1，物品2，同现频次）
    val user_rdd8 = user_rdd7.map(f => (f._1._1, (f._1._1, f._1._2, f._2))).
      join(user_rdd6.map(f => (f._1._1, f._2)))
    val user_rdd9 = user_rdd8.map(f => (f._2._1._2, (f._2._1._1,
      f._2._1._2, f._2._1._3, f._2._2)))
    val user_rdd10 = user_rdd9.join(user_rdd6.map(f => (f._1._1, f._2)))
    val user_rdd11 = user_rdd10.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))
    val user_rdd12 = user_rdd11.map(f => (f._1, f._2, (f._3 / sqrt(f._4 * f._5))))
    // 6 结果返回
    user_rdd12.map(f => ItemSimi(f._1, f._2, f._3))
  }

  /**
    * 余弦相似度矩阵计算.
    * T(x,y) = ∑x(i)y(i) / sqrt(∑(x(i)*x(i)) * ∑(y(i)*y(i)))
    *
    * @param user_rdd 用户评分
    * @param RDD[ItemSimi] 返回物品相似度
    *
    */
  def CosineSimilarity(user_rdd: RDD[ItemPref]): (RDD[ItemSimi]) = {
    // 0 数据做准备
    val user_rdd1 = user_rdd.map(f => (f.userid, f.itemid, f.pref))
    val user_rdd2 = user_rdd1.map(f => (f._1, (f._2, f._3)))
    // 1 (用户,物品,评分) 笛卡尔积 (用户,物品,评分) => （物品1,物品2,评分1,评分2）组合
    val user_rdd3 = user_rdd2.join(user_rdd2)
    val user_rdd4 = user_rdd3.map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2)))
    // 2 （物品1,物品2,评分1,评分2）组合 => （物品1,物品2,评分1*评分2） 组合 并累加
    val user_rdd5 = user_rdd4.map(f => (f._1, f._2._1 * f._2._2)).reduceByKey(_ + _)
    // 3 对角矩阵
    val user_rdd6 = user_rdd5.filter(f => f._1._1 == f._1._2)
    // 4 非对角矩阵
    val user_rdd7 = user_rdd5.filter(f => f._1._1 != f._1._2)
    // 5 计算相似度
    val user_rdd8 = user_rdd7.map(f => (f._1._1, (f._1._1, f._1._2, f._2))).
      join(user_rdd6.map(f => (f._1._1, f._2)))
    val user_rdd9 = user_rdd8.map(f => (f._2._1._2, (f._2._1._1,
      f._2._1._2, f._2._1._3, f._2._2)))
    val user_rdd10 = user_rdd9.join(user_rdd6.map(f => (f._1._1, f._2)))
    val user_rdd11 = user_rdd10.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))
    val user_rdd12 = user_rdd11.map(f => (f._1, f._2, (f._3 / sqrt(f._4 * f._5))))
    // 6 结果返回
    user_rdd12.map(f => ItemSimi(f._1, f._2, f._3))
  }

  /**
    * 欧氏距离相似度矩阵计算.
    * d(x, y) = sqrt(∑((x(i)-y(i)) * (x(i)-y(i))))
    * sim(x, y) = n / (1 + d(x, y))
    *
    * @param user_rdd 用户评分
    * @param RDD[ItemSimi] 返回物品相似度
    *
    */
  def EuclideanDistanceSimilarity(user_rdd: RDD[ItemPref]): (RDD[ItemSimi]) = {
    // 0 数据做准备
    val user_rdd1 = user_rdd.map(f => (f.userid, f.itemid, f.pref))
    val user_rdd2 = user_rdd1.map(f => (f._1, (f._2, f._3)))
    // 1 (用户,物品,评分) 笛卡尔积 (用户,物品,评分) => （物品1,物品2,评分1,评分2）组合
    val user_rdd3 = user_rdd2 join user_rdd2
    val user_rdd4 = user_rdd3.map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2)))
    // 2 （物品1,物品2,评分1,评分2）组合 => （物品1,物品2,评分1-评分2） 组合 并累加
    val user_rdd5 = user_rdd4.map(f => (f._1, (f._2._1 - f._2._2) * (f._2._1 - f._2._2))).reduceByKey(_ + _)
    // 3 （物品1,物品2,评分1,评分2）组合 => （物品1,物品2,1） 组合 并累加    计算重叠数
    val user_rdd6 = user_rdd4.map(f => (f._1, 1)).reduceByKey(_ + _)
    // 4 非对角矩阵
    val user_rdd7 = user_rdd5.filter(f => f._1._1 != f._1._2)
    // 5 计算相似度
    val user_rdd8 = user_rdd7.join(user_rdd6)
    val user_rdd9 = user_rdd8.map(f => (f._1._1, f._1._2, f._2._2 / (1 + sqrt(f._2._1))))
    // 6 结果返回
    user_rdd9.map(f => ItemSimi(f._1, f._2, f._3))
  }



  /*分别计算每个user的(item,item)相似性的分子
  输入x=(user,(item1,item2,...))
  * */
  //1个user如果有100个商品，则产生100*100对tuple,相当于笛卡尔
  def compute_numerator(item_seq :Iterable[Int] )={
    val item_cnt = item_seq.map(t=> 1).reduce(_+_)
    for (item1 <- item_seq;
         item2<- item_seq;
         if item1 != item2)
      yield { ((item1,item2),1.0/math.log(1 + item_cnt))}
  }

  def compute_numerator2(item_seq :Iterable[Int] )={
    //val item_cnt = item_seq.map(t=> 1).reduce(_+_)
    for (item1 <- item_seq;
         item2<- item_seq;
         if item1 != item2)
      yield { ((item1,item2),1)}
  }

  /**

  def compute_sim( user_action :RDD[ItemPref] ,part_num:Int  ,k :Int): RDD[ItemSimi] ={

    //为了节省内存，将item由字符串转换为int型。
    val item_map = user_action .map (t=> t.itemid).distinct(part_num/10).zipWithIndex().map(t=> (t._1,t._2.toInt)).cache()
    print("item count is " +item_map.count() )

    val user_item = user_action.map(t=>(t.itemid,t.userid))
      .join(item_map)
      .map{case (item,(user,item_num)) => (user,item_num)}

    val item_freq = user_item.map{t=>(t._2,1)}.reduceByKey(_+_).persist(StorageLevel.MEMORY_AND_DISK )
    item_freq.first()
    // return (user,Array(item1,item2,...))
    val user_vectors = user_item.groupByKey().persist(StorageLevel.DISK_ONLY )
    print("user count is " +user_vectors.count() )

    //
    val sim_numerator = user_vectors.flatMap{x=> compute_numerator(x._2)}.reduceByKey(_+_)

    //compute item pair interaction
    val item_join= user_vectors.flatMap{x=> compute_numerator2(x._2)}.reduceByKey(_+_)

    //计算(item,item)相似性计算的分母，即并集.返回 ((item_1,item2),score)
    //并集=item1集合 + item2集合 - 交集
    val sim_denominator = item_join.map{ case ((item1,item2),num) => (item1,(item2,num)) }
      .join(item_freq).map{case  (item1,((item2,num),fre1)) => (item2,(item1,fre1,num))}
      .join(item_freq).map{case  (item2,((item1,fre1,num),fre2)) =>
      ((item1,item2) , fre1 + fre2 - num)
    }
    //相似度
    val sim= sim_numerator.join(sim_denominator).map{
      case((item1,item2),(num,denom)) => (item1,item2 ,num/denom )}

    //topk
    val pair_topk = sim.map{case (item1,item2,score) => (item1,(item2,score))}.groupByKey().flatMap{
      case( a, b)=>  //b=Interable[(item2,score)]
        val topk= b.toArray.sortWith{ (a,b) => a._2>b._2 }.take(k)
        topk.map{ t => (a,t._1,t._2) }
    }

    //标准化得分 breeze.linalg.normalize
    val value_max= pair_topk.map( _._3 ).max()
    val value_min= 0 // not equal 0 ,but approximation 0
    print("max score =" + value_max)

    val similary = pair_topk.map{case(item1,item2,score)=>(item1,item2,math.round(100.0*(score-value_min)/(value_max - value_min))/100.0 )}.filter(t=>t._3>0)

    //转换回Long
    val similary_res = similary.map(t=> (t._1,(t._2,t._3)))
      .join(item_map.map(_.swap))
      .map{case (id1,((id2,score),item1)) => (id2,(item1,score))}
      .join(item_map.map(_.swap))
      .map{case (id2,((item1,score),item2)) => (item2,item1,score)}

    //return
    similary.map(t=>ItemSimi(t._1,t._2,t._3))
  }

    **/

}
