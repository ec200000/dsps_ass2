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

public class Step3 {

    // taking the 3-gram and counting number of times each word occurs.
    // each line from the 3-gram is:
    // ngram TAB year TAB occurrences TAB pages TAB books NEWLINE
    // Input and output for example: "Bait Gadol Meod" 2000 5 1 1 => "Bait Gadol Meod" 5

    private static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split("\t");
            if (vals[0].split(" ").length > 2) {
                Text words = new Text();
                words.set(vals[0]);
                Text occurrences = new Text();
                occurrences.set(String.format("%d", Integer.parseInt(vals[2])));
                context.write(words, occurrences);
            }
        }
    }

    // The reducer sums all the occurrences of the words
    // Input and output For example: "Bait Gadol Meod" 5 "Bait Gadol Meod" 3 => "Bait Gadol Meod" 8

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                sum += Long.parseLong(val.toString());
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
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        String output = "/output3/";
        SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}