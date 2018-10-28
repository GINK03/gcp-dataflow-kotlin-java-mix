
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
//import org.apache.beam.sdk.io.PubsubIO
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
//import org.apache.beam.sdk.options
//import org.apache.beam.sdk.options.DataflowPipelineOptions
//import org.apache.beam.sdk.options.PipelineOptionsFactory
//import org.apache.beam.sdk.runners.BlockingDataflowPipelineRunner
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.*
import org.apache.beam.sdk.transforms.ParDo                                                                                                        
import org.apache.beam.sdk.values.KV 
import org.apache.beam.sdk.transforms.GroupByKey

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
//import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions

val KotlinHelloString : String = "Hello from Kotlin!"

fun getHelloStringFromJava() : String {
    return "call from kotlin;"
}

public class KotlinProc1 : DoFn<String, String>() {
  @ProcessElement 
  fun processElement(c : DoFn<String,String>.ProcessContext) {
    val elem = c.element()
    elem.toList().map { 
      val char = it.toString()
      c.output(char)
    }
  }
}

public class KotlinProc2 : DoFn<String, KV<String, Int>>() {
  @ProcessElement 
  fun processElement(c : ProcessContext) {
    val char = c.element()
    c.output(KV.of(char, 1))
  }
}

public class KotlinProc3 : DoFn<KV<String, Iterable<Int>>, String>() {
  @ProcessElement 
  fun processElement(c : ProcessContext) {
    val key = c.element().getKey()
    val iter = c.element().getValue()
    val list = mutableListOf<String>()
    iter.forEach {  list.add(it.toString()) }
    c.output(key.toString() + ": " + list.size.toString()); 
  }
}

fun main( args : Array<String> ) {
  val options = JavaDataFlowUtils.getOptionInstance()
  // define project name
  options.setProject("wild-yukikaze")
  // define staging directory
  options.setStagingLocation( "gs://abc-wild/STAGING" )
  // args order, 1st -> options, 2nd -> input data bucket, 3rd -> output data bucket
  runner(options, "testSub1", "gs://abc-wild/OUTPUTS/OUTPUT" )
}

fun runner(options: DataflowPipelineOptions, input:String, output: String) {
  val p:Pipeline = Pipeline.create(options)
  p.apply( TextIO.read().from(input) )
    .apply( ParDo.of( KotlinProc1() ) )
    .apply( ParDo.of( KotlinProc2() ) )
    .apply( GroupByKey.create<String,Int>() )
    .apply( ParDo.of( KotlinProc3() ) )
    .apply( TextIO.write().to(output) )
  p.run().waitUntilFinish()
}
