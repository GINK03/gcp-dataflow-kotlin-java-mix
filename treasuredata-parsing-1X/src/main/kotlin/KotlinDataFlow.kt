
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
    if( recover.get("gender_age") == null )      
      return
    if( recover.get("income") == null )      
      return
    //if( recover.get("os") == null)               
    //  return
    if( recover.get("tuuid") == null || recover.get("tuuid")!! == "null")
      return
    val gender_age = (recover["gender_age"]!! as Double).toInt()                                   
    val income = (recover["income"] as Double).toInt() 
    //val os = recover["os"]!! as String           
    val uuid = recover["tuuid"] as String  
    val data_owner_id = (recover["data_owner_id"]!! as Double).toInt()
    val urlreq = try {                           
      "ipao9702=(.*?)&".toRegex().find( 
          URLDec.decode( URLDec.decode( recover["request_uri"]!! as String, "UTF-8"), "UTF-8" ) )?.groups?.get(1)?.value ?: null                                                          
    } catch( ex : Exception ) { 
      null 
    }
    if( urlreq == null || urlreq == "" )
      return
    val output = gson.toJson( listOf("${data_owner_id}", "${gender_age}", "${income}", "${uuid}", "${urlreq}") )
    //println(output)
    c.output(output)
  }
}

public class KotlinProc2 : DoFn<String, KV<String, String>>() {
  override fun processElement(c : DoFn<String, KV<String,String>>.ProcessContext) {
    val gson = Gson()                              
    val type = object : TypeToken<List<String>>() {}.type 

    val recover:List<String> = gson.fromJson<List<String>>(c.element(), type)                      
    println(recover)
    val (data_owner_id, genderage, income, uuid, keyword) = recover
    c.output(KV.of("${data_owner_id + "_" + keyword + "_" + genderage + "_" + income}", gson.toJson(listOf(genderage, income, uuid))))
  }
}
public class KotlinProc3 : DoFn<KV<String, Iterable<String>>, String>() {
  override fun processElement(c : DoFn<KV<String, Iterable<String>>, String>.ProcessContext) {
    val gson = Gson()                              
    val type = object : TypeToken<List<String>>() {}.type 

    val key = c.element().getKey()
    val iter = c.element().getValue()
    val valset = mutableSetOf<String>()
    iter.forEach {  
      val value:List<String> = gson.fromJson<List<String>>(it.toString(), type)
      val tuuid = value[2]
      valset.add(tuuid) 
    }
    // キーの粒度はkeyword + data_owner_id
    c.output(key.toString() + "\t" + gson.toJson(valset.toList()) )
  }
}

fun main( args : Array<String> ) {
  val options = JavaDataFlowUtils.getOptionInstance()
  // define project name
  options.setProject("machine-learning-173502")
  // define max workers
  options.setMaxNumWorkers(80)
  // define staging directory
  options.setStagingLocation( "gs://dataflow-stagings-machine-learning/stating-08" )
  // args order, 1st -> options, 2nd -> input data bucket, 3rd -> output data bucket
  runner(options, "gs://treasuredata-dump/20171221-json/export.*.*.jsonl.gz", "gs://dataflow-output-machine-learning/genderage_os_uuid-08/*" )
}

fun runner(options: DataflowPipelineOptions, input:String, output: String) {
  val p:Pipeline = Pipeline.create(options)
  p.apply( TextIO.Read.from(input) )
    .apply( ParDo.named("ExtractMap1").of( KotlinProc1() ) )
    .apply( ParDo.named("MakeTransit").of( KotlinProc2() ) )
    .apply( GroupByKey.create<String,String>() )
    .apply( ParDo.named("FormatResults").of( KotlinProc3() ) )
    .apply( TextIO.Write.named("Result").to(output) )
  p.run()
}
