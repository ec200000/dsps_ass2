import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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

public class Step5 {

    // The mapper separates the 3-grams and the values

    private static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings = value.toString().split("\t");
            String[] words = strings[0].split(" ");
            String[] val = strings[1].split(" ");
            String first = words[0], second = words[1], third = words[2];
            Text textKey = new Text();
            textKey.set(String.format("%s %s %s", first, second, third));
            if (val.length >= 2) {
                Text textValueFirst = new Text();
                textValueFirst.set(String.format("%s %s %d", val[0], val[1], Integer.parseInt(val[2])));
                context.write(textKey, textValueFirst);
            } else {
                Text textValueSecond = new Text();
                textValueSecond.set(String.format("%d", Integer.parseInt(strings[1])));
                context.write(textKey, textValueSecond);
            }
        }
    }

    // The reducer calculates the probability for each 3-gram to appear
    // Input and output for example: "Bait Gadol Meod | Bait Gadol" 5 "Bait Gadol Meod" | 8 => "Bait Gadol Meod" 0.008

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        public static HashMap<String, Double> wordsOccurs = new HashMap<String, Double>();
        public static Long C0 = 0L;

        public void setup(Reducer.Context context) throws IOException {
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(new Path("/output1"), false);
            while (remoteIterator.hasNext()) {
                LocatedFileStatus fileStatus = remoteIterator.next();
                if (fileStatus.getPath().getName().startsWith("part")) { // hadoop output file
                    FSDataInputStream InputStream = fileSystem.open(fileStatus.getPath());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(InputStream, "UTF-8"));
                    String line = null;
                    String[] oneGram;
                    while ((line = reader.readLine()) != null) {
                        oneGram = line.split("\t");
                        if (oneGram[0].equals("*")) {
                            C0 = Long.parseLong(oneGram[1]);
                        } else {
                            wordsOccurs.put(oneGram[0], (double) Long.parseLong(oneGram[1]));
                        }
                    }
                    reader.close();
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Double N2 = 0.0, N3 = 0.0, C2 = 0.0, k2 = 0.0, k3 = 0.0, p = 0.0;
            String[] vals = key.toString().split(" ");
            String first = vals[0], second = vals[1], third = vals[2];
            Double N1 = wordsOccurs.get(third);
            Double C1 = wordsOccurs.get(second);
            Text textKey = new Text(), textValue = new Text();
            int counter = 0;
            for (Text value : values) {
                String[] val = value.toString().split(" ");
                if (val.length <= 1) {
                    N3 = (double) Long.parseLong(val[0]);
                    k3 = (Math.log(N3 + 1) + 1) / (Math.log(N3 + 1) + 2);
                } else {
                    if (val[0].equals(first) && val[1].equals(second)) {
                        counter++;
                        C2 = (double) Long.parseLong(val[2]);
                    } else {
                        counter++;
                        if (val[0].equals(second) && val[1].equals(third))
                            N2 = (double) Long.parseLong(val[2]);
                        k2 = (Math.log(N2 + 1) + 1) / (Math.log(N2 + 1) + 2);
                    }
                }
                if (N1 != null && C1 != null && counter >= 2) {
                    p = (k3 * (N3 / C2)) + ((1 - k3) * k2 * (N2 / C1)) + ((1 - k3) * (1 - k2) * (N1 / C0));
                    textKey.set(String.format("%s %s %s", first, second, third));
                    textValue.set(String.format("%s", p));
                    context.write(textKey, textValue);
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
        job.setJarByClass(Step5.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String input1 = "/output4/";
        String input2 = "/output3/";
        String output = "/output5/";
        MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}