package com.ljw.function

import com.ljw.utils.KeyWordUtil
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row
import scala.collection.JavaConversions._

@FunctionHint(output = new DataTypeHint("ROW<word String>"))
class IKSplitFunction extends TableFunction[Row]{
    def eval(str: String): Unit = {

      val strings = KeyWordUtil.analyze(str)
      for(str <- strings){
        collect(Row.of(str))
      }
    }

}
