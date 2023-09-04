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

public class Task2Bigram {
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
    private Text tokenized_text = new Text();
    private Text input_data_documentID = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] document_data = value.toString().split("\t", 2);

      String input_texts = document_data[1].toLowerCase();
      input_texts = input_texts.replaceAll("[^a-z\\s]", " ");
      input_texts = input_texts.replaceAll("\\s+", " ");

      input_data_documentID.set(document_data[0]);
      StringTokenizer tokenizer = new StringTokenizer(input_texts);
      String previous = tokenizer.nextToken();
      while (tokenizer.hasMoreTokens()) {
        String current = tokenizer.nextToken();
        if ((previous.equals("computer") && current.equals("science"))
            || (previous.equals("information") && current.equals("retrieval"))
            || (previous.equals("power") && current.equals("politics"))
            || (previous.equals("los") && current.equals("angeles"))
            || (previous.equals("bruce") && current.equals("willis"))) {
          tokenized_text.set(previous + " " + current);
          context.write(tokenized_text, input_data_documentID);
        }
        previous = current;
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
    Job job = Job.getInstance(conf, "Inverted Index Bigrams");
    job.setJarByClass(Task2Bigram.class);
    job.setMapperClass(Task2Bigram.TokenizerMapper.class);
    job.setReducerClass(Task2Bigram.IndexReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}