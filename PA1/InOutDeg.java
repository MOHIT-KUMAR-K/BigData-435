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
import java.util.StringTokenizer;

;

public class InOutDeg {
    public static class InOutMapper extends Mapper<Object, Text, Text, Text> {


        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String itr = value.toString();
            String[] itr_split = itr.split(" ");

            String n1 = itr_split[0];
            String n2 = itr_split[1];

            Text node1 = new Text(n1);
            Text node2 = new Text(n2);


            context.write(node1, new Text("out"));
            context.write(node2, new Text("in"));


        }
    }

    public static class InOutReducer extends Reducer<Text, Text, Text, Text> {
        int counter = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
            int out_counts = 0;
            int in_counts = 0;

            while (values.iterator().hasNext()) {
                Text value = (Text) values.iterator().next();
                String value_str = value.toString();

                if (value_str.equals("out"))
                    out_counts++;
                else if (value_str.equals("in"))
                    in_counts++;

            }

            Text out = new Text(in_counts + " " + out_counts);
            if (counter < 100) {
                context.write(key, out);
                counter++;
            } else {
                counter++;
//            context.write(key,out);
            }

            }

        }


//        public static class InOutMapper2 extends Mapper<Object, Text, Text, NullWritable> {
//            @Override
//            protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//                StringTokenizer itr = new StringTokenizer(value.toString());
//                while (itr.hasMoreTokens()) {
//                    String s1 = itr.nextToken();
//                    String s2 = itr.nextToken();
//                    String s3 = itr.nextToken();
//                    Text node = new Text(String.valueOf(s1) + " " + String.valueOf(s2) + " " + String.valueOf(s3));
//                    context.write(node, NullWritable.get());
//                }
//
//
//            }
//
//        }
//
//
//        public static class InOutReducer2 extends Reducer<Text, NullWritable, Text, NullWritable> {
//            int counter = 0;
//
//            @Override
//            protected void reduce(Text key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException {
//
//                while (value.iterator().hasNext()) {
//                    if (counter <= 10) {
//                        context.write(key, NullWritable.get());
//                        counter++;
//                    } else {
//                        counter++;
//                    }
//                }
//
//
//            }
//
//
//        }


        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "InOut degree new");
            job.setJarByClass(InOutDeg.class);
            job.setMapperClass(InOutMapper.class);
            job.setReducerClass(InOutReducer.class);
            job.setNumReduceTasks(1);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);

//            Configuration conf2 = new Configuration();
//            Job job2 = Job.getInstance(conf2, "InOut degree new2");
//            job2.setJarByClass(InOutDeg.class);
//            job2.setMapperClass(InOutMapper2.class);
//            job2.setReducerClass(InOutReducer2.class);
//            job2.setNumReduceTasks(1);
//            job2.setMapOutputKeyClass(Text.class);
//            job2.setMapOutputValueClass(NullWritable.class);
//            job2.setOutputKeyClass(Text.class);
//            job2.setOutputValueClass(NullWritable.class);
//            FileInputFormat.addInputPath(job2, new Path(args[1]));
//            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
//            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }