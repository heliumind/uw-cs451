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
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfStringInt;
import tl.lin.data.pair.PairOfWritables;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Iterator;
import java.util.List;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

    private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, VIntWritable> {
        private static final PairOfStringInt TERM_ID = new PairOfStringInt();
        private static final VIntWritable FREQ = new VIntWritable();
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

    private static final class MyPartitioner extends Partitioner<PairOfStringInt, VIntWritable> {
        @Override
        public int getPartition(PairOfStringInt key, VIntWritable value, int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    private static final class MyReducer extends
            Reducer<PairOfStringInt, VIntWritable, Text, BytesWritable> {
        private static final BytesWritable BYTES = new BytesWritable();
        private static final ArrayListWritable<PairOfWritables<VIntWritable, VIntWritable>> postings = new ArrayListWritable<>();
        private static final Text T_PREV = new Text("");
        private static final VIntWritable DID_PREV = new VIntWritable(0);

        private static final ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        private static final DataOutputStream dataStream = new DataOutputStream(byteArrayStream);

        @Override
        public void reduce(PairOfStringInt key, Iterable<VIntWritable> values, Context context)
                throws IOException, InterruptedException {
            if (!T_PREV.toString().equals("") && !key.getLeftElement().equals(T_PREV.toString())) {
                for (PairOfWritables<VIntWritable, VIntWritable> pair : postings) {
                    // Write first docID (left) and frequency (right)
                    pair.getLeftElement().write(dataStream);
                    pair.getRightElement().write(dataStream);
                }
                postings.clear();

                // EMIT postings
                BYTES.set(byteArrayStream.toByteArray(), 0, byteArrayStream.size());
                context.write(T_PREV, BYTES);

                // Reset streams and DID_PREV
                DID_PREV.set(0);
                byteArrayStream.reset();
                dataStream.flush();
            }

            Iterator<VIntWritable> iter = values.iterator();

            int tf = 0;
            while (iter.hasNext()) {
                tf += iter.next().get();
            }

            int delta = key.getRightElement() - DID_PREV.get();

            VIntWritable TF = new VIntWritable(tf);
            VIntWritable DELTA = new VIntWritable(delta);
            postings.add(new PairOfWritables<>(DELTA, TF));

            DID_PREV.set(key.getRightElement());
            T_PREV.set(key.getLeftElement());
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (PairOfWritables<VIntWritable, VIntWritable> pair : postings) {
                // Write first docID (left) and frequency (right)
                pair.getLeftElement().write(dataStream);
                pair.getRightElement().write(dataStream);
            }
            postings.clear();
            BYTES.set(byteArrayStream.toByteArray(), 0, byteArrayStream.size());
            if (DID_PREV.get() > 0) context.write(T_PREV, BYTES);
            DID_PREV.set(0);
            byteArrayStream.reset();
            dataStream.flush();
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

        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path(args.input));
        FileOutputFormat.setOutputPath(job, new Path(args.output));

        job.setMapOutputKeyClass(PairOfStringInt.class);
        job.setMapOutputValueClass(VIntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairOfWritables.class);
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
