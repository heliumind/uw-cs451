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

package ca.uwaterloo.cs451.a3;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfStringInt;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Iterator;
import java.util.List;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

    private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
        private static final PairOfStringInt TERM_ID = new PairOfStringInt();
        private static final IntWritable FREQ = new IntWritable();
        private static final Object2IntFrequencyDistribution<String> COUNTS =
                new Object2IntFrequencyDistributionEntry<>();

        @Override
        public void map(LongWritable docno, Text doc, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(doc.toString());

            // Build a histogram of the terms.
            COUNTS.clear();
            for (String token : tokens) {
                COUNTS.increment(token);
            }

            // Emit postings.
            for (PairOfObjectInt<String> e : COUNTS) {
                FREQ.set(e.getRightElement());
                TERM_ID.set(e.getLeftElement(), (int) docno.get());
                context.write(TERM_ID, FREQ);
            }
        }
    }

    private static final class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
        @Override
        public int getPartition(PairOfStringInt key, IntWritable value, int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    private static final class MyReducer extends
            Reducer<PairOfStringInt, IntWritable, Text, BytesWritable> {
        private static final Text TEXT = new Text();

        private static final ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        private static final DataOutputStream dataStream = new DataOutputStream(byteArrayStream);

        int df = 0;
        String prevTerm = "";
        int prevDocID = 0;

        @Override
        public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            if (!prevTerm.equals("") && !key.getLeftElement().equals(prevTerm)) {
                byteArrayStream.flush();
                dataStream.flush();

                ByteArrayOutputStream byteArrayBuf = new ByteArrayOutputStream(byteArrayStream.size());
                DataOutputStream dataBuf = new DataOutputStream(byteArrayBuf);
                WritableUtils.writeVInt(dataBuf, df);
                dataBuf.write(byteArrayStream.toByteArray());

                // EMIT postings
                TEXT.set(prevTerm);
                context.write(TEXT, new BytesWritable(byteArrayBuf.toByteArray()));

                prevDocID = 0;
                df = 0;
                byteArrayStream.reset();
            }

            Iterator<IntWritable> iter = values.iterator();

            while (iter.hasNext()) {
                int delta = key.getRightElement() - prevDocID;
                WritableUtils.writeVInt(dataStream, delta);
                WritableUtils.writeVInt(dataStream, iter.next().get());
                prevDocID = key.getRightElement();
                df++;
            }

            prevTerm = key.getLeftElement();
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            byteArrayStream.flush();
            dataStream.flush();

            ByteArrayOutputStream byteArrayBuf = new ByteArrayOutputStream(byteArrayStream.size());
            DataOutputStream dataBuf = new DataOutputStream(byteArrayBuf);
            WritableUtils.writeVInt(dataBuf, df);
            dataBuf.write(byteArrayStream.toByteArray());
            TEXT.set(prevTerm);
            context.write(TEXT, new BytesWritable(byteArrayBuf.toByteArray()));

            dataStream.close();
            byteArrayStream.close();
        }
    }

    private BuildInvertedIndexCompressed() {}

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        String output;

        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        int numReducers = 1;
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

        LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - num reducers: " + args.numReducers);

        Job job = Job.getInstance(getConf());
        job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
        job.setJarByClass(BuildInvertedIndexCompressed.class);

        job.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job, new Path(args.input));
        FileOutputFormat.setOutputPath(job, new Path(args.output));

        job.setMapOutputKeyClass(PairOfStringInt.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setOutputFormatClass(MapFileOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setPartitionerClass(MyPartitioner.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
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
        ToolRunner.run(new BuildInvertedIndexCompressed(), args);
    }
}
