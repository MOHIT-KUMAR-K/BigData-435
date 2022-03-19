import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;



public class FriendsNetwork {
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

    //
    //
    //
    //
    //
    //
    //
    //
    //


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

            //Text out1 = new Text(node1+" "+node2);
            //Text out2 = new Text(node2+" "+node1);

            if(cmp<0){
                context.write(node1,node2);
                }
            else{
                context.write(node2,node1);
            }


        }

    }


    public static class FriendsNetworkReducer2 extends Reducer<Text, Text, Text, Text> {


        int counter = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> list1 = new ArrayList<String>();

            while (values.iterator().hasNext()) {
                Text value = (Text) values.iterator().next();
                String value_str = value.toString();
                if (list1.contains(value_str) && counter < 100) {
                    context.write(key, new Text(value_str));
                    counter++;
                } else
                    list1.add(value_str);
            }
        }




    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }

}


//    public static class FriendsNetworkReducer3 extends Reducer<Text, NullWritable, Text, NullWritable> {
//
//        int counter =0;
//        @Override
//        protected void reduce(Text key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException {
//
//            while(value.iterator().hasNext()) {
//                if (counter <= 10) {
//                    context.write(key,(NullWritable) value);
//                    counter ++;
//                }
//                else{
//                    counter++;
//                }
//            }
//
//
//        }
//
//    }





    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "friends n/w");
        job.setJarByClass(FriendsNetwork.class);
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
        job2.setJarByClass(FriendsNetwork.class);
        job2.setMapperClass(FriendsNetworkMapper2.class);
        job2.setReducerClass(FriendsNetworkReducer2.class);
        job2.setNumReduceTasks(1);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);


//        Configuration conf3 = new Configuration();
//        Job job3 = Job.getInstance(conf3, "friends n/w3");
//        job3.setJarByClass(FriendsNetwork.class);
//        //job3.setMapperClass(FriendsNetworkMapper3.class);
//        job3.setReducerClass(FriendsNetworkReducer3.class);
//        job3.setNumReduceTasks(1);
//        job3.setMapOutputKeyClass(Text.class);
//        job3.setMapOutputValueClass(NullWritable.class);
//        job3.setOutputKeyClass(Text.class);
//        job3.setOutputValueClass(NullWritable.class);
//        FileInputFormat.addInputPath(job3, new Path(args[2]));
//        FileOutputFormat.setOutputPath(job3, new Path(args[3]));
//        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}