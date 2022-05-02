import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step6 {

    // The mapper doesn't do anything

    private static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split("\t");
            Text textKey = new Text();
            textKey.set(String.format("%s %s",vals[0],vals[1]));
            Text textValue = new Text();
            textValue.set(String.format("%s",""));
            context.write(textKey,textValue);
        }
    }

    // The reducer doesn't do anything

    private static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text textValue = new Text();
            textValue.set(String.format("%s", ""));
            context.write(key, textValue);
        }
    }

    // The comparator sorts the data by the value

    private static class CompareClass extends WritableComparator {
        protected CompareClass() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable key1, WritableComparable key2) {
            String[] first = key1.toString().split(" ");
            String[] second = key2.toString().split(" ");
            if (first[0].equals(second[0]) && first[1].equals(second[1])) {
                if (Double.parseDouble(first[3]) > (Double.parseDouble(second[3]))) {
                    return -1;
                } else
                    return 1;
            }
            return (first[0] + " " + first[1]).compareTo(second[0] + " " + second[1]);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step6.class);
        job.setMapperClass(MapperClass.class);
        job.setSortComparatorClass(CompareClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String input = "/output5/";
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(1); // single reducer so only one output file
        job.waitForCompletion(true);
    }
}