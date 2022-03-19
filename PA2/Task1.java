import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;



public class Task1 {
    public static class FriendsNetworkMapper extends Mapper<Object, Text, Text, NullWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String s1 = itr.nextToken();
                String s2 = itr.nextToken();
                Text node = new Text(String.valueOf(s1) + " " + String.valueOf(s2));
                context.write(node, NullWritable.get());
            }


        }
    }

    public static class FriendsNetworkReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());


        }

    }


    public static class FriendsNetworkMapper2 extends Mapper<Object, Text, Text, Text> {


        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String[] str_split = str.split(" ");


            String n1 = str_split[0];
            String n2 = str_split[1];

            int cmp = n1.compareTo(n2);


            Text node1 = new Text(n1);
            Text node2 = new Text(n2);

            if(cmp<0){
                context.write(node1,node2);
            }
            else{
                context.write(node2,node1);
            }


        }

    }


    public static class FriendsNetworkReducer2 extends Reducer<Text, Text, Text, Text> {



        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> list1 = new ArrayList<String>();

            while (values.iterator().hasNext()) {
                Text value = (Text) values.iterator().next();
                String value_str = value.toString();
                if (list1.contains(value_str)) {


                } else {
                    list1.add(value_str);
                    context.write(key, new Text(value_str));
                }
            }
        }




        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

        }

    }


    public static class EdgeCountMapper extends Mapper<Object, Text, IntWritable, Text>
    {        private Text count;
        private final static IntWritable x = new IntWritable(1);


        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens())
            {
                String s = itr.nextToken();
                context.write(new IntWritable(1),new Text(s));
                String s1 = itr.nextToken();
                context.write(new IntWritable(1),new Text(s1));



            }
        }
    }
    public static class CountReducer extends Reducer<IntWritable, Text,Text, IntWritable> {
        private IntWritable vertices = new IntWritable();
        ArrayList<Text> al = new ArrayList<>();


        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text t : values){
                Text value = (Text) t;

                if (al.contains(value)) {

                } else{
                    al.add(new Text(value));

                }
//                context.write(new IntWritable(1),value);


            }

        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Vertices"),new IntWritable(al.size()));

        }

    }


    public static class EdgeCounter extends Mapper<Object, Text, Text, NullWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String s1 = itr.nextToken();
                String s2 = itr.nextToken();
                Text node = new Text(String.valueOf(s1) + " " + String.valueOf(s2));
                context.write(node, NullWritable.get());
            }


        }
    }

    public static class edgereducer extends Reducer<Text, NullWritable, Text, IntWritable> {
        int counter;
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            counter++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("edges"),new IntWritable(counter));

        }

    }







    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task1");
        job.setJarByClass(Task1.class);
        job.setMapperClass(FriendsNetworkMapper.class);
        job.setReducerClass(FriendsNetworkReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);


        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "friends n/w 2");
        job2.setJarByClass(Task1.class);
        job2.setMapperClass(FriendsNetworkMapper2.class);
        job2.setReducerClass(FriendsNetworkReducer2.class);
        job2.setNumReduceTasks(1);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);

        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "countEdges new");
        job3.setJarByClass(Task1.class);
        job3.setMapperClass(EdgeCountMapper.class);
        job3.setReducerClass(CountReducer.class);
        job3.setNumReduceTasks(1);
        job3.setMapOutputKeyClass(IntWritable.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, new Path(args[2]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));
        job3.waitForCompletion(true);


        Configuration conf4 = new Configuration();
        Job job4 = Job.getInstance(conf4, "edge counter");
        job4.setJarByClass(Task1.class);
        job4.setMapperClass(EdgeCounter.class);
        job4.setReducerClass(edgereducer.class);
        job4.setNumReduceTasks(1);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(NullWritable.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job4, new Path(args[2]));
        FileOutputFormat.setOutputPath(job4, new Path(args[4]));
        System.exit(job4.waitForCompletion(true) ? 0 : 1);


    }
}