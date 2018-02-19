
import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner
import com.google.cloud.dataflow.sdk.transforms.Count
import com.google.cloud.dataflow.sdk.transforms.Create
import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.transforms.DoFn.*
import com.google.cloud.dataflow.sdk.transforms.ParDo
import com.google.cloud.dataflow.sdk.transforms.Flatten
import com.google.cloud.dataflow.sdk.values.PCollection
import com.google.cloud.dataflow.sdk.values.PCollectionList

import com.google.cloud.dataflow.sdk.values.KV 
import com.google.cloud.dataflow.sdk.transforms.GroupByKey
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import java.net.URLDecoder as URLDec             
import java.lang.Exception as Exception 
val KotlinDataFlowProjectName : String = "TreasureData Parsing!"


public class KotlinProc1 : DoFn<String, String>() {
  fun parseAone(elem: String) : String? {
    val gson = Gson()                              
    val type = object : TypeToken<Map<String, Any>>() {}.type 
    
    val recover:Map<String,Any> = gson.fromJson<Map<String, Any>>(elem, type)                      
    if( recover.get("tuuid") == null || recover.get("tuuid")!! == "null")
      return null
    if( recover.get("time") == null || recover.get("time")!! == "null") 
      return null
    if( recover.get("url_category_ids") == null || recover.get("url_category_ids")!! == "null") 
      return null
    if( recover.get("data_owner_id") == null ) 
      return null
    val unix_time = (recover["time"] as Double).toInt()
    val category_id = recover["url_category_ids"] as String
    val uuid = recover["tuuid"] as String  
    val data_owner_id = recover["data_owner_id"] as Double
    if( uuid == "" || category_id == "" || unix_time == 0 || uuid[0] != '7') 
      return null
    val skeyword:String? = try {
      "ipao9702=(.*?)&".toRegex().find(
          URLDec.decode( URLDec.decode( recover["request_uri"]!! as String, "UTF-8"), "UTF-8" ) )?.groups?.get(1)?.value ?: null
    } catch( ex : Exception ) {
      null
    }
    val source:String? = try {
      "src=(.*?)&".toRegex().find(
          URLDec.decode( URLDec.decode( recover["request_uri"]!! as String, "UTF-8"), "UTF-8" ) )?.groups?.get(1)?.value ?: null
    } catch( ex : Exception ) {
      null
    }
    val mkeywords:List<String>? = try {
      "mtk=(.*?)&".toRegex().findAll(
          URLDec.decode( URLDec.decode( recover["request_uri"]!! as String, "UTF-8"), "UTF-8" ) ).toList().map { it.value } ?: null
    } catch( ex : Exception ) {
      null
    }
    val output = gson.toJson( listOf(uuid, unix_time.toString(), category_id, skeyword, mkeywords, data_owner_id, source, "meta_aone") )
    return output
  }

  fun parseDac(elem: String) : List<String>? {
    val gson = Gson()                              
    val type = object : TypeToken<Map<String, Any>>() {}.type 
    
    val recover:Map<String,Any> = gson.fromJson<Map<String, Any>>(elem, type)                      
    val time = recover["date_time"] as String
    val t_category_ids = recover["t_category_ids"] as List<String>
    val uuid = recover["tuuid"] as String  
    val outputs = t_category_ids.map { t_category_id ->
      gson.toJson( listOf(uuid, time, t_category_id, null, null, -1, null, "meta_dac") )
    }
    return outputs
  }

  override fun processElement(c : DoFn<String,String>.ProcessContext) {
    val gson = Gson()                              
    val type = object : TypeToken<Map<String, Any>>() {}.type 

    val elem = c.element()
    val identify:Map<String,Any> = gson.fromJson<Map<String, Any>>(elem, type)                      
    if( identify.get("identifier") != null ) {
      //println(elem)
      //println(identify)
      val outputs = parseDac(elem) 
      println(outputs)
      if( outputs == null ) return 
      outputs.map { c.output(it) }    
    } else {
      val output = parseAone(elem) 
      if( output == null ) return 
      c.output(output)
    }
  }
}

public class KotlinProc2 : DoFn<String, KV<String, String>>() {
  override fun processElement(c : DoFn<String, KV<String,String>>.ProcessContext) {
    val gson = Gson()                              
    val type = object : TypeToken<List<Any>>() {}.type 

    val recover:List<Any> = gson.fromJson<List<Any>>(c.element(), type)                      
    println(recover)
    val uuid = recover[0] as String
    val time = recover[1] as String
    val category_id = recover[2] as String
    val skeyword = recover[3] as String? 
    val metakeywords = recover[4] as List<String>? 
    val data_owner_id = recover[5] as Double
    val source = recover[6] as String?
    val meta = recover[7] as String
    val value = gson.toJson( listOf(time, category_id, skeyword, metakeywords, data_owner_id, source, meta) )
    if( uuid != "" ) 
      c.output(KV.of(uuid, value) )
  }
}
public class KotlinProc3 : DoFn<KV<String, Iterable<String>>, String>() {
  override fun processElement(c : DoFn<KV<String, Iterable<String>>, String>.ProcessContext) {
    val gson = Gson()                              
    val type = object : TypeToken<List<Any>>() {}.type 
    try {
      val key = c.element().getKey()
      val iter = c.element().getValue()
      var map = mutableMapOf<String, MutableList<List<Any>>>()
      iter.forEach {  
        val value:List<Any> = gson.fromJson(it.toString(), type)
        val data_owner_id = (value[4] as Double).toInt()
        val mini_key = "${key}_${data_owner_id}"
        if( map.get(mini_key) == null ) 
          map[mini_key] = mutableListOf<List<Any>>()
        map[mini_key]!!.add(value)
      }
      // キーの粒度はkeyword + data_owner_id
      val gson = Gson()                              
      c.output(key.toString() + "\t" + gson.toJson(map) )
    } catch ( ex : Exception ) { 
      println("Exception ${ex}")
    }
  }
}

fun main( args : Array<String> ) {
  val options = JavaDataFlowUtils.getOptionInstance()
  // define project name
  options.setProject("machine-learning-173502")
  // define max workers
  options.setMaxNumWorkers(128)
  // disk size 
  options.setDiskSizeGb(1024*2)
  // machine type
  options.setWorkerMachineType("n1-highmem-4")
  // define staging directory
  options.setStagingLocation( "gs://dataflow-stagings-machine-learning/stating-36" )
  // args order, 1st -> options, 2nd -> input data bucket, 3rd -> output data bucket
  runner(options, "gs://treasuredata-dump/20171221-json/export.*", 
                  "gs://dataflow-output-machine-learning/keyword_uuid_timeseries-categories-17/*" )
}

fun runner(options: DataflowPipelineOptions, input:String, output: String) {
  val p:Pipeline = Pipeline.create(options)

  val events1:PCollection<String> = p.apply(TextIO.Read.from(input));
  val events2:PCollection<String> = p.apply(TextIO.Read.from("gs://dac-user-enhance2/*"));
  val eventsList = PCollectionList.of(events1).and(events2)
  val events = eventsList.apply(Flatten.pCollections())
  //p.apply( TextIO.Read.from(input) )
  events
    .apply( ParDo.named("ExtractMap1").of( KotlinProc1() ) )
    .apply( ParDo.named("MakeTransit").of( KotlinProc2() ) )
    .apply( GroupByKey.create<String,String>() )
    .apply( ParDo.named("FormatResults").of( KotlinProc3() ) )
    .apply( TextIO.Write.to(output) )
  p.run()
}
