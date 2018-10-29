
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;

public class JavaDataFlowUtils {
  public static DataflowPipelineOptions getOptionInstance() {
    DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    options.setRunner(BlockingDataflowPipelineRunner.class);
    return options;
  }
}
