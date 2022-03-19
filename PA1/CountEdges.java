import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.StringTokenizer;

public class CountEdges {
    public static class EdgeCountMapper extends Mapper<Object, Text, IntWritable, IntWritable>
    {        private Text count;
    private final static IntWritable x = new IntWritable(1);


        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens())
            {
                String s = itr.nextToken();
                count =new Text( s );
                s = itr.nextToken();
                context.write(x, x);
            }


        }
    }
    public static class CountReducer extends Reducer<IntWritable, IntWritable,NullWritable, IntWritable> {
        private IntWritable edges = new IntWritable();


        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            edges.set(sum);
            context.write(NullWritable.get(),edges);
        }

    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "countEdges new");
        job.setJarByClass(CountEdges.class);
        job.setMapperClass(EdgeCountMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}