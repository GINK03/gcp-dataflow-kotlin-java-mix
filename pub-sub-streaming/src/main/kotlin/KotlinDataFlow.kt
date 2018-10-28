
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


val KotlinHelloString : String = "Hello from Kotlin!"

fun getHelloStringFromJava() : String {
    return "call from kotlin;"
}

public class KotlinProc1 : DoFn<String, String>() {
  override fun processElement(c : DoFn<String,String>.ProcessContext) {
    val elem = c.element()
    elem.toList().map { 
      val char = it.toString()
      c.output(char)
    }
  }
}

public class KotlinProc2 : DoFn<String, KV<String, String>>() {
  override fun processElement(c : DoFn<String, KV<String,String>>.ProcessContext) {
    val char = c.element()
    c.output(KV.of(char, "1"))
  }
}
public class KotlinProc3 : DoFn<KV<String, Iterable<String>>, String>() {
  override fun processElement(c : DoFn<KV<String, Iterable<String>>, String>.ProcessContext) {
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
  runner(options, "gs://dataflow-samples/shakespeare/*", "gs://abc-wild/OUTPUT" )
}

fun runner(options: DataflowPipelineOptions, input:String, output: String) {
  val p:Pipeline = Pipeline.create(options)
  p.apply( TextIO.Read.from(input) )
    .apply( ParDo.named("ExtractWords1").of( KotlinProc1() ) )
    .apply( ParDo.named("make kv").of( KotlinProc2() ) )
    .apply( GroupByKey.create<String,String>() )
    .apply( ParDo.named("FormatResults").of( KotlinProc3() ) )
    .apply( TextIO.Write.to(output) )
  p.run()
}
