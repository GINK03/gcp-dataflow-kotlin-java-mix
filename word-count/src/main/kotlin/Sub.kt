
@file:JvmName("funcs")
@file:JvmMultifileClass
package kt

import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.transforms.SerializableFunction
public class KProc1():DoFn<String, String>() {
  @ProcessElement
  public fun processElement(@Element line:String, rec:OutputReceiver<String>) {
    line.split(" ").map {
      rec.output( it.trim(' ').trim('\t').replace(",", "").replace(".", "").replace("?", "").replace("'", "") )
    }
  }
}

public class KProc2():DoFn<String, KV<String, Int>>() {
  @ProcessElement
  public fun processElement(@Element word:String, rec:OutputReceiver<KV<String, Int>>) {
    rec.output(KV.of(word.toLowerCase(), 1))
  }
}

/*
public class KProc4() : DoFn<KV<String, Iterable<Int>>, String>() {
  @ProcessElement
  public fun processElement(@Element kv:KV<String, Iterable<Int>>, rec:OutputReceiver<String>) {
    val key = kv.getValue()
    rec.output("s")
  }
}
*/
public fun kproc4(kv:KV<String, Iterable<Int>>, rec:OutputReceiver<String>) {
  val key = kv.getKey().toString()
  val iters = kv.getValue().toString()
  rec.output(key + ":" + iters)
}


public fun filter1(input:String):Boolean {
  if(input == " " || input == "") {
    return false
  } else {
    return true
  }
}
