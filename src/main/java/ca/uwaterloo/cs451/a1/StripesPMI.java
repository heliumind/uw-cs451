/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.map.HMapStFW;
import tl.lin.data.map.HashMapWritable;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.pair.PairOfFloats;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);
  private static final int MAX_NUM_WORDS = 40;

  // Mapper1 counts word occurrences and retrieves marginals (with In-mapping combining)
  private static final class MyMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Map<String, Integer> counts;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      counts = new HashMap<>();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());
      tokens = tokens.subList(0, Math.min(MAX_NUM_WORDS, tokens.size()));
      // Remove duplicates from tokens
      List<String> tokensDedup = tokens.stream().distinct().collect(Collectors.toList());
      for (String token : tokensDedup) {
        if (counts.containsKey(token)) {
          counts.put(token, counts.get(token) + 1);
        } else {
          counts.put(token, 1);
        }
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable COUNT = new IntWritable();
      Text WORD = new Text();

      for (Map.Entry<String, Integer> entry : counts.entrySet()) {
        WORD.set(entry.getKey());
        COUNT.set(entry.getValue());
        context.write(WORD, COUNT);
      }
    }
  }

  public static final class MyReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  // Mapper2 is the second pass which calculates PMI
  private static final class MyMapper2 extends Mapper<LongWritable, Text, Text, HMapStFW> {
    private static final Text TEXT = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      Map<String, HMapStFW> stripes = new HashMap<>();

      List<String> tokens = Tokenizer.tokenize(value.toString());
      tokens = tokens.subList(0, Math.min(MAX_NUM_WORDS, tokens.size()));
      List<String> tokensDedup = tokens.stream().distinct().collect(Collectors.toList());

      if (tokensDedup.size() < 2) return;
      for (int i = 1; i < tokensDedup.size(); i++) {
        String prev = tokensDedup.get(i - 1);
        String cur = tokensDedup.get(i);
        if (stripes.containsKey(prev)) {
          HMapStFW stripe = stripes.get(prev);
          if (stripe.containsKey(cur)) {
            stripe.put(cur, stripe.get(cur) + 1.0f);
          } else {
            stripe.put(cur, 1.0f);
          }
        } else {
          HMapStFW stripe = new HMapStFW();
          stripe.put(cur, 1.0f);
          stripes.put(prev, stripe);
        }
      }

      for (String t : stripes.keySet()) {
        TEXT.set(t);
        context.write(TEXT, stripes.get(t));
      }
    }
  }

  private static final class MyCombiner2 extends Reducer<Text, HMapStFW, Text, HMapStFW> {
    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
            throws IOException, InterruptedException {
      Iterator<HMapStFW> iter = values.iterator();
      HMapStFW map = new HMapStFW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }

  private static final class MyReducer2 extends Reducer<Text, HMapStFW, Text, HashMapWritable> {
    private static final HashMapWritable<Text, PairOfFloatInt> WORD = new HashMapWritable<>();
    private static final PairOfFloatInt PMICOUNT = new PairOfFloatInt();
    private static Map<String, Integer> wcMap = new HashMap<>();
    private static long lineCnt = 1l;
    private static int threshold = 10;

    @Override
    public void setup(Context context) throws IOException {
      lineCnt = context.getConfiguration().getLong("lineCnt", 1l);
      threshold =  context.getConfiguration().getInt("threshold", 10);

      URI[] cacheFiles = context.getCacheFiles();

      if (cacheFiles != null && cacheFiles.length > 0) {
        try {
          FileSystem fs = FileSystem.get(context.getConfiguration());
          Path wcMapPath = new Path(cacheFiles[0].toString());
          BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(wcMapPath)));

          // Custom parser
          String line = "";
          while ((line = reader.readLine()) != null) {
            String[] array = line.split("\\s+");
            if (array.length == 2) {
              wcMap.put(array[0], Integer.parseInt(array[1]));
            } else {
              LOG.info("wcPMI corrupted");
            }
          }
        } catch (Exception e) {
          LOG.info("Unable to read wcPMI");
          e.printStackTrace();
        }
      }
    }

    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStFW> iter = values.iterator();
      HMapStFW map = new HMapStFW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      boolean valid = false;
      // Calculate PMI for every stripe
      String x = key.toString();
      for (String y : map.keySet()) {
        float xyCount = map.get(y);
        // Check threshold and division-by-zero
        if (xyCount >= threshold && lineCnt != 0 && wcMap.get(x) != null && wcMap.get(y) != null) {
          float xCount = wcMap.get(x);
          float yCount = wcMap.get(y);
          float numerator = xyCount / lineCnt;
          float denominator = (xCount / lineCnt) * (yCount / lineCnt);
          float pmi = (float) Math.log10(numerator / denominator);
          PMICOUNT.set(pmi, (int) xyCount);
          WORD.put(new Text(y), PMICOUNT);
          valid = true;
        }
      }
      if (valid) context.write(key, WORD);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-threshold", metaVar = "[num]", usage = "minimum co-occurrence count")
    int threshold = 10;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - threshold: " + args.threshold);

    // Job 1: Word count
    Job job1 = Job.getInstance(getConf());
    job1.setJobName(StripesPMI.class.getSimpleName());
    job1.setJarByClass(StripesPMI.class);

    job1.setNumReduceTasks(1);
    String sideOutputPath = "wcPMI";
    Path sideOutputDir = new Path(sideOutputPath);
    FileSystem.get(getConf()).delete(sideOutputDir, true);

    FileInputFormat.setInputPaths(job1, new Path(args.input));
    FileOutputFormat.setOutputPath(job1, new Path(sideOutputPath));

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    job1.setMapperClass(MyMapper1.class);
    job1.setReducerClass(MyReducer1.class);

    job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);

    // Use Map input records counter to get number of total lines
    long mapInputRecord = job1.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
    LOG.info("Total lines read:" + mapInputRecord);

    System.out.println("Job1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    // Job 2:
    Job job2 = Job.getInstance(getConf());
    job2.setJobName(StripesPMI.class.getSimpleName());
    job2.setJarByClass(StripesPMI.class);

    job2.getConfiguration().setInt("threshold", args.threshold);
    job2.getConfiguration().setLong("lineCnt", mapInputRecord);

    job2.setNumReduceTasks(args.numReducers);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    FileInputFormat.setInputPaths(job2, new Path(args.input));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(HMapStFW.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(HashMapWritable.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    job2.setMapperClass(MyMapper2.class);
    job2.setCombinerClass(MyCombiner2.class);
    job2.setReducerClass(MyReducer2.class);

    // Add wcPMI marginals to distributed cache
    // https://www.geeksforgeeks.org/distributed-cache-in-hadoop-mapreduce/
    try {
      Path wcPMIFile = new Path(sideOutputDir + "/part-r-00000");
      job2.addCacheFile(wcPMIFile.toUri());
    } catch (Exception e) {
      LOG.info("File Not Added");
      System.exit(1);
    }

    job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    long startTime2 = System.currentTimeMillis();
    job2.waitForCompletion(true);

    System.out.println("Job2 Finished in " + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}
