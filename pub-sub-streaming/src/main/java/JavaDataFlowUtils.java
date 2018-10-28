
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
//import org.apache.beam.runners.dataflow.BlockingDataflowPipelineRunner;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;

public class JavaDataFlowUtils {
  public static String getHelloStringFromKotlin() {
    return KotlinDataFlowKt.getKotlinHelloString();
  }
  
  public static DataflowPipelineOptions getOptionInstance() {
    DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    //options.setRunner(BlockingDataflowPipelineRunner.class);
    return options;
  }

  public class JavaProc3 extends DoFn<KV<String, Iterable<Integer>>, String> {
    @ProcessElement
    public void processElement(ProcessContext c) { 
      
      c.output("dummy");
    }
  }
  /* 
  public static void runner(DataflowPipelineOptions options, String input, String output) {
    Pipeline p = Pipeline.create(options);
    p.apply( TextIO.read().from(input) )
      .apply( ParDo.of( new KotlinDataFlowKt.KotlinProc1() ) ) 
      .apply( ParDo.of( new KotlinDataFlowKt.KotlinProc2() ) )
      .apply( GroupByKey.<String,Integer>create() ) 
      .apply( ParDo.of( new KotlinDataFlowKt.KotlinProc3() ) ) 
      .apply( TextIO.write().to(output) ); 
    p.run().waitUntilFinish();
  }
  */
}

