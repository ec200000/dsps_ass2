import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step1 {

    // taking the 1-gram and counting number of times each word occurs.
    // each line from the 1-gram is:
    // ngram TAB year TAB occurrences TAB pages TAB books NEWLINE
    // Input and output for example: "Bait" 2000 5 1 1 => "Bait" 5

    private static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split("\t");
            String oneGram = vals[0];
            if (!(oneGram.equals("*"))) {
                Text word = new Text();
                word.set(String.format("%s", oneGram));
                Text occurrences = new Text(), asterisk = new Text();
                asterisk.set(String.format("*"));
                occurrences.set(String.format("%d", Integer.parseInt(vals[2])));
                context.write(word, occurrences);
                context.write(asterisk, occurrences);
            }
        }
    }

    // The reducer sums all the occurrences of the word
    // Input and output For example: "Bait" 5 "Bait" 3 => "Bait" 8

    private static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                sum += Integer.parseInt(val.toString());
            }
            Text value = new Text();
            value.set(String.format("%d", sum));
            context.write(key, value);
        }
    }

    private static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        String output = "/output1/";
        SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}