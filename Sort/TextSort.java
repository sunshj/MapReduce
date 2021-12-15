package MapReduce.Sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class TextSort {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: TextSort <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Merge and Sort");
        job.setJarByClass(TextSort.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setSortComparatorClass(MyComparator.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {

        private static final IntWritable data = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            data.set(Integer.parseInt(line));
            context.write(data, new IntWritable(1));

        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private static IntWritable line_num = new IntWritable(1);

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            for (IntWritable num : values) {
                context.write(line_num, key);
                line_num = new IntWritable(line_num.get() + 1);
            }

        }
    }

    public static class MyComparator extends WritableComparator {
        public MyComparator() {
            super(IntWritable.class, true);
        }

        @Override
        @SuppressWarnings({ "rawtypes", "unchecked" }) // 不检查类型
        public int compare(WritableComparable a, WritableComparable b) {
            // CompareTo方法，返回值为1则降序，-1则升序
            // 默认是a.compareTo(b)，a比b小返回-1，现在反过来返回1，就变成了降序
            return b.compareTo(a);
        }

    }
}
