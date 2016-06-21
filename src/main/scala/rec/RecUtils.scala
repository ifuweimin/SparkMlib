package rec

import com.alibaba.fastjson.JSON
import com.test.DESKey

/**
  * Created by fuweimin on 2016/6/20.
  */
object RecUtils {

  def get_detail(line: String) : String = {
    //String param = "{\"dxid\":\""+dxid+"\",\"datatype\":\""+datatype+"\",\"d\":\""+md5ToDxid(dxid)+"\"}";
    val detail_json = DESKey.decrypt(line.replaceAll("/detail_",""), "1")
    val detail_obj = JSON.parseObject(detail_json)
    val dxid = detail_obj.getString("dxid")
    val datatype = detail_obj.getInteger("datatype")
    val rowkey = String.format("%03d", datatype) + dxid
    rowkey
  }

}
