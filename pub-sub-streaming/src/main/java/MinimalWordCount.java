import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import  org.apache.beam.sdk.transforms.windowing.Window;
import  org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.joda.time.Duration;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
class JProc1 extends DoFn<KV<String, Iterable<Integer>>,String> {
  @ProcessElement
  public void processElement(@Element KV<String,Iterable<Integer>> kv, OutputReceiver<String> out) { 
    kt.funcs.kproc4(kv, out); 
    //String key = kv.getKey().toString();
    //out.output(key);
  }
}

public class MinimalWordCount {
  public static void main(String[] args) {
    kt.funcs.testCall();
    DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    options.setProject("wild-yukikaze");
    options.setStagingLocation("gs://abc-wild/STAGING");
		options.setTempLocation("gs://abc-wild/tmp");
		options.setRunner(DataflowRunner.class);
		options.setStreaming(true);
    options.setJobName("streamingJob");

    Pipeline p = Pipeline.create(options);
    PCollection p1 = p.apply(PubsubIO.readStrings().fromSubscription("projects/wild-yukikaze/subscriptions/testSub1"))
        .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(5))));
    POutput p2 = p1.apply( TextIO.write()
                .withWindowedWrites()
                .withNumShards(10)
                .to("gs://abc-wild/OUTPUT2") );
        //.apply( ParDo.of(new kt.KProc1()))
        //.apply( Filter.by( (String chars) -> kt.funcs.filter1(chars) ))
        //.apply( ParDo.of(new kt.KProc2()))
        //.apply( GroupByKey.create())
        //.apply( ParDo.of(new JProc1()) );
    //POutput p2 = p1.apply( TextIO.write().to("gs://abc-wild/OUTPUT2") );
    p.run().waitUntilFinish();
    
  }
}
