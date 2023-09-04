import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

public class Task1Unigram {
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
    private Text tokenized_text = new Text();
    private Text input_data_documentID = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] input_data = value.toString().split("\t", 2);

      String input_texts = input_data[1].toLowerCase();
      input_texts = input_texts.replaceAll("[^a-z\\s]", " ");
      input_texts = input_texts.replaceAll("\\s+", " ");

      input_data_documentID.set(input_data[0]);
      StringTokenizer tokenizer = new StringTokenizer(input_texts);
      while (tokenizer.hasMoreTokens()) {
        tokenized_text.set(tokenizer.nextToken());
        context.write(tokenized_text, input_data_documentID);
      }
    }
  }

  public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      HashMap<String, Integer> count = new HashMap<>();
      for (Text val : values) {
        String input_data_documentID = val.toString();
        count.put(input_data_documentID, count.getOrDefault(input_data_documentID, 0) + 1);
      }

      StringBuilder s = new StringBuilder();
      for (String k : count.keySet())
        s.append(k).append(":").append(count.get(k)).append("\t");

      result.set(s.substring(0, s.length() - 1));
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Inverted Index");
    job.setJarByClass(Task1Unigram.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IndexReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
