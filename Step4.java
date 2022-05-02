import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step4 {

    // The mapper separates the 3-gram to the first and second pair concat with the 3-gram and occurrences
    // Input and output for example: "Bait Gadol Meod" 8 => "Bait Gadol Bait Gadol Meod" 8 "Gadol Meod Bait Gadol Meod" 8
    // Input and output for example: "Bait Gadol" 5 => "Bait Gadol" 5

    private static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split("\t");
            String[] words = vals[0].split(" ");
            String first = words[0], second = words[1];
            Text occurrences = new Text();
            occurrences.set(String.format("%d", Integer.parseInt(vals[1])));
            Text firstPair = new Text();
            firstPair.set(String.format("%s %s", first, second));
            if (words.length > 2) {
                String third = words[2];
                Text threeGram = new Text();
                threeGram.set(String.format("%s %s %s %d", first, second, third, Integer.parseInt(vals[1])));
                Text secondPair = new Text();
                secondPair.set(String.format("%s %s", second, third));
                context.write(secondPair, threeGram);
                context.write(firstPair, threeGram);
            } else {
                context.write(firstPair, occurrences);
            }
        }
    }

    // The reducer separates the 3-gram the pair and the occurrences and puts the right occurrence
    // Input and output for example: "Bait Gadol Bait Gadol Meod" 8 "Bait Gadol" 5 => "Bait Gadol Meod Bait Gadol" 5

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] strings = key.toString().split(" ");
            String first = strings[0], second = strings[1];
            Text textKey = new Text();
            Text textValue = new Text();
            boolean flag1 = false, flag2 = false;
            for (Text value : values) {
                String[] vals = value.toString().split(" ");
                if (vals.length <= 1) {
                    int occurs = (int) Long.parseLong(vals[0]);
                    textValue.set(String.format("%s %s %d", first, second, occurs));
                    flag1 = true;
                } else {
                    textKey.set(String.format("%s %s %s", vals[0], vals[1], vals[2]));
                    flag2 = true;
                }
                if (flag1 && flag2) {
                    context.write(textKey, textValue);
                    flag2 = false;
                }
            }
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
        job.setJarByClass(Step4.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String input1 = "/output2/";
        String input2 = "/output3/";
        String output = "/output4/";
        MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}