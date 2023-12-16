import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SalesByShop {

    public static class CombinedMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        Text outkey = new Text();
        DoubleWritable cout = new DoubleWritable();
        HashMap<String, String> hmap = new HashMap<String, String>();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] part = line.split("\t");
            outkey.set(part[2]);
            cout.set(Double.parseDouble(part[4]));
            context.write(outkey, cout);
        }
    }

    public static class CombinedReducer extends Reducer<Text, DoubleWritable , Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable > values, Context context)
                throws IOException, InterruptedException {
            double salesTotal = 0;
            for (DoubleWritable  val : values) {
                double value = Double.parseDouble(val.toString());
                salesTotal += value;
            }
            result.set(salesTotal);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.printf("Usage: CombinedDriver <input dir> <output dir>\n");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "ventes");
        job.setMapperClass(CombinedMapper.class);
        job.setReducerClass(CombinedReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setJarByClass(SalesByShop.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJobName("Combined Driver");

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}