
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
    val request_uri = try{ 
      URLDec.decode( URLDec.decode( recover["request_uri"]!! as String, "UTF-8"), "UTF-8" ) 
    } catch( ex : Exception ) { 
      val ret = try {
        URLDec.decode( recover["request_uri"]!! as String, "UTF-8") 
      } catch( ex : Exception ) {
        null
      }
      ret
    }
    if( request_uri == null ) return
    val uuid = recover["tuuid"] as String  
    val data_owner_id = (recover["data_owner_id"]!! as Double).toInt()
    val skeyword:String? = try {                           
      "ipao9702=(.*?)&".toRegex().find( 
          request_uri )?.groups?.get(1)?.value ?: null                                                          
    } catch( ex : Exception ) { 
      null 
    }
    /*val mkeywords:List<String>? = try {
      "mtk=(.*?)&".toRegex().findAll( 
          request_uri ).toList().map { it.value } ?: null                                                          
    } catch( ex : Exception ) { 
      null 
    }*/
    //val output = gson.toJson( listOf(uuid, skeyword, mkeywords) )
    if( skeyword == null || skeyword == "" ) 
      return
    val output = gson.toJson( listOf(uuid, skeyword) )
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
    val skeyword = recover[1] as String
    //val mkeywords = recover[2] as List<String>?
    
    if( skeyword != null )
      c.output(KV.of(skeyword, uuid))
    /*if( mkeywords != null ) { 
      mkeywords!!.map{ 
        c.output(KV.of(it, uuid))
      }
    }*/
  }
}
public class KotlinProc3 : DoFn<KV<String, Iterable<String>>, String>() {
  override fun processElement(c : DoFn<KV<String, Iterable<String>>, String>.ProcessContext) {
    val gson = Gson()                              
    val type = object : TypeToken<List<String>>() {}.type 
    try {
      val key = c.element().getKey()
      val iter = c.element().getValue()
      var valset = mutableSetOf<String>()
      iter.forEach {  
        val uuid:String = it.toString()
        valset.add(uuid) 
        if( valset.size >= 10000 ) {
          c.output(key.toString() + "\t" + gson.toJson(valset.toList()) )
          valset = mutableSetOf<String>()
        }
      }
      // キーの粒度はkeyword + data_owner_id
      c.output(key.toString() + "\t" + gson.toJson(valset.toList()) )
    } catch ( ex: Exception ) {
    }
  }
}

fun main( args : Array<String> ) {
  val options = JavaDataFlowUtils.getOptionInstance()
  // define project name
  options.setProject("machine-learning-173502")
  // define max workers
  options.setMaxNumWorkers(128)
  // define staging directory
  options.setStagingLocation( "gs://dataflow-stagings-machine-learning/stating-54" )
  // args order, 1st -> options, 2nd -> input data bucket, 3rd -> output data bucket
  runner(options, "gs://treasuredata-dump/20171221-json/export.[0-9]*.jsonl.gz", "gs://dataflow-output-machine-learning/keyword_uuid-08-part-0-9/*" )
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
