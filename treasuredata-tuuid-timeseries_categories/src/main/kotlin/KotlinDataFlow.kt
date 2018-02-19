
import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner
import com.google.cloud.dataflow.sdk.transforms.Count
import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.transforms.DoFn.*
import com.google.cloud.dataflow.sdk.transforms.ParDo                                                                                                        
import com.google.cloud.dataflow.sdk.values.KV 
import com.google.cloud.dataflow.sdk.transforms.GroupByKey
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import java.net.URLDecoder as URLDec             
import java.lang.Exception as Exception 
val KotlinDataFlowProjectName : String = "TreasureData Parsing!"


public class KotlinProc1 : DoFn<String, String>() {
  override fun processElement(c : DoFn<String,String>.ProcessContext) {
    val gson = Gson()                              
    val type = object : TypeToken<Map<String, Any>>() {}.type 

    val elem = c.element()
    val recover:Map<String,Any> = gson.fromJson<Map<String, Any>>(elem, type)                      
    //if( recover.get("gender_age") == null )      
    //  return
    //if( recover.get("income") == null )      
    //  return
    //if( recover.get("os") == null)               
    //  return
    //val gender_age = (recover["gender_age"]!! as Double).toInt()                                   
    //val income = (recover["income"] as Double).toInt() 
    //val os = recover["os"]!! as String           
    if( recover.get("tuuid") == null || recover.get("tuuid")!! == "null")
      return
    if( recover.get("time") == null || recover.get("time")!! == "null") 
      return
    if( recover.get("url_category_ids") == null || recover.get("url_category_ids")!! == "null") 
      return
    val unix_time = (recover["time"] as Double).toInt()
    val category_id = recover["url_category_ids"] as String
    val uuid = recover["tuuid"] as String  
    if( uuid == "" || category_id == "" || unix_time == 0) 
      return
    //val data_owner_id = (recover["data_owner_id"]!! as Double).toInt()
    /* val skeyword:String? = try {                           
      "ipao9702=(.*?)&".toRegex().find( 
          URLDec.decode( URLDec.decode( recover["request_uri"]!! as String, "UTF-8"), "UTF-8" ) )?.groups?.get(1)?.value ?: null                                                          
    } catch( ex : Exception ) { 
      null 
    }
    val mkeywords:List<String>? = try {
      "mtk=(.*?)&".toRegex().findAll( 
          URLDec.decode( URLDec.decode( recover["request_uri"]!! as String, "UTF-8"), "UTF-8" ) ).toList().map { it.value } ?: null                                                          
    } catch( ex : Exception ) { 
      null 
    } */
    val output = gson.toJson( listOf(uuid, unix_time, category_id) )
    c.output(output)
  }
}

public class KotlinProc2 : DoFn<String, KV<String, String>>() {
  override fun processElement(c : DoFn<String, KV<String,String>>.ProcessContext) {
    val gson = Gson()                              
    val type = object : TypeToken<List<Any>>() {}.type 

    val recover:List<Any> = gson.fromJson<List<Any>>(c.element(), type)                      
    println(recover)
    val uuid = recover[0] as String
    val unix_time = (recover[1] as Double).toInt()
    val category_id = recover[2] as String

    val value = unix_time.toString() + "_" + category_id
    if( uuid != "" ) 
      c.output(KV.of(uuid, value) )
  }
}
public class KotlinProc3 : DoFn<KV<String, Iterable<String>>, String>() {
  override fun processElement(c : DoFn<KV<String, Iterable<String>>, String>.ProcessContext) {
    try {
      val key = c.element().getKey()
      val iter = c.element().getValue()
      var map = mutableMapOf<Int,String>()
      iter.forEach {  
        val value:List<String> = it.toString().split("_")
        val unix_time = (value[0]).toInt()
        val category_id = value[1]
        map[unix_time] = category_id
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
  options.setDiskSizeGb(1024*5)
  // machine type
  options.setWorkerMachineType("n1-highmem-4")
  // define staging directory
  options.setStagingLocation( "gs://dataflow-stagings-machine-learning/stating-30" )
  // args order, 1st -> options, 2nd -> input data bucket, 3rd -> output data bucket
  runner(options, "gs://treasuredata-dump/20171221-json/export.[0-9][0-9]*.jsonl.gz", "gs://dataflow-output-machine-learning/keyword_uuid_timeseries-categories-10-0-9-0-9/*" )
}

fun runner(options: DataflowPipelineOptions, input:String, output: String) {
  val p:Pipeline = Pipeline.create(options)
  p.apply( TextIO.Read.from(input) )
    .apply( ParDo.named("ExtractMap1").of( KotlinProc1() ) )
    .apply( ParDo.named("MakeTransit").of( KotlinProc2() ) )
    .apply( GroupByKey.create<String,String>() )
    .apply( ParDo.named("FormatResults").of( KotlinProc3() ) )
    .apply( TextIO.Write.to(output) )
  p.run()
}
