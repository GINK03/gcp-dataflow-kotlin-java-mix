
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package MinimalWordCount;


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
    //kt.funcs.filter1(" ");
    
    DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    options.setProject("wild-yukikaze");
    options.setStagingLocation("gs://abc-wild/STAGING");
		options.setTempLocation("gs://abc-wild/TMP");
		options.setRunner(DataflowRunner.class);
		options.setStreaming(true);
    options.setJobName("streamingJob");

    Pipeline p = Pipeline.create(options);
    PCollection p1 = p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/*"))
        .apply( ParDo.of(new kt.KProc1()))
        .apply( Filter.by( (String chars) -> kt.funcs.filter1(chars) ))
        .apply( ParDo.of(new kt.KProc2()))
        .apply( GroupByKey.create())
        .apply( ParDo.of(new JProc1()) );
    POutput p2 = p1.apply( TextIO.write().to("gs://abc-wild/OUTPUT") );
    p.run().waitUntilFinish();
    
  }
}
