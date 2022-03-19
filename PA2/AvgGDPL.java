import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.*;


public class AvgGDPL {
    public static class AvgGDPLMapper1 extends Mapper<Object, Text, Text, Text> {
        String s1;
        String s2;
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokens = new StringTokenizer(value.toString());
            while (tokens.hasMoreTokens()) {
                s1 = tokens.nextToken();
                s2 = tokens.nextToken();
                Text outk = new Text(s1 + " " + s2);
                Text outv = new Text(s1 + " " + s2);
                context.write(outk,outv);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {


        }
    }

    public static class AvgGDPLoriginalMapper extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
           StringTokenizer str = new StringTokenizer(value.toString());
           while(str.hasMoreTokens()){
               outkey.set(str.nextToken());
               outvalue.set(str.nextToken());
               context.write(outkey,outvalue);
           }

        }
    }

    public static class AvgGDPLMapper2 extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] val = value.toString().split(" ");
            String s1 = val[0];
            String s2 = val[1];
            String comb_txt ="d" + val[0]+val[1]+val[2] + val[3];

            outkey.set(s2);
            outvalue.set(comb_txt);
            context.write(outkey,outvalue);
        }
    }



    public static class AvgGDPLReducer2
            extends Reducer<Text, Text, Text, Text> {
        String orig = null;
        private Text outkey = new Text();
        private Text outvalue = new Text();
        private Text outvaluefinal = new Text();
        private List list1;
        private List list2;
        String[] str;


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text t : values){
                str = t.toString().split(" ");
                        if(str.length==1) {
                            list1.add(str);
                        }
                        else{
                            //str1.add(str);
                        }


                    outkey.set(t.toString().substring(1,t.toString().length()).trim());

            }


        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AvgGDPL");
        job.setJarByClass(AvgGDPL.class);
        job.setMapperClass(AvgGDPL.AvgGDPLMapper1.class);
        //job.setReducerClass(AvgGDPL.AvgGDPLReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ArrayList.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "AvgGDPL2");
        job2.setJarByClass(AvgGDPL.class);
        //job2.setMapperClass(AvgGDPL.AvgGDPLMapper2.class);
        MultipleInputs.addInputPath(job2,new Path(args[1]),TextInputFormat.class,AvgGDPLMapper2.class);
        MultipleInputs.addInputPath(job2,new Path(args[0]),TextInputFormat.class,AvgGDPLoriginalMapper.class);
        //job.setReducerClass(AvgGDPL.AvgGDPLReducer2.class);
        job2.setNumReduceTasks(1);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(ArrayList.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);


    }
}

