import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task3{
    public static class EdgeReadermapper extends Mapper<Object, Text, LongWritable, LongWritable> {

        private LongWritable outkey = new LongWritable();
        private LongWritable outvalue = new LongWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer edgeIterator = new StringTokenizer(value.toString());
            while (edgeIterator.hasMoreTokens()) {
                outkey.set(Long.parseLong(edgeIterator.nextToken()));
                outvalue.set(Long.parseLong(edgeIterator.nextToken()));
                context.write(outkey, outvalue);
            }
        }
    }
    public static class Degree1Reducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
        private LongWritable keyOut = new LongWritable();
        private Text node = new Text();

        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            int size = 0;

            long[] nbrs = new long[4096];

            Iterator<LongWritable> neighborIterator = values.iterator();

            while (neighborIterator.hasNext()) {
                if (nbrs.length == size) {
                    nbrs = Arrays.copyOf(nbrs, (int) (size * 1.5));
                }
                long neighbor = neighborIterator.next().get();
                nbrs[size++] = neighbor;
            }

            for (int i = 0; i < size; i++) {
                long neighborId = nbrs[i];
                keyOut.set(neighborId);
                node.set(key.toString() + "," + Integer.toString(size));
                context.write(keyOut, node);
            }

        }
    }

    public static class EdgeReader2 extends Mapper<Object, Text, LongWritable, Text> {

        private LongWritable vertex = new LongWritable();
        private Text node = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer edgeIterator = new StringTokenizer(value.toString());
            while (edgeIterator.hasMoreTokens()) {
                vertex.set(Long.parseLong(edgeIterator.nextToken()));
                if (!edgeIterator.hasMoreTokens()) {
                    throw new RuntimeException("Invalid edge in EdgeReader.");
                }
                node.set(edgeIterator.nextToken().toString());
                context.write(vertex, node);
            }
        }
    }

    public static class Degree2Reducer extends Reducer<LongWritable, Text, Text, Text> {
        private Text node1 = new Text();
        private Text node2 = new Text();

        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String[] nbrs = new String[4096];
            int size = 0;
            for (Text text : values) {
                if (nbrs.length == size) {
                    nbrs = Arrays.copyOf(nbrs, (int) (size * 1.5));
                }
                String neighbor = text.toString();
                nbrs[size++] = new String(neighbor);
            }

            node1.set(key.toString() + "," + Integer.toString(size));

            long id1 = Long.parseLong(key.toString());
            for (int i = 0; i < size; i++) {
                String[] neighbor = nbrs[i].split(",");
                long id2 = Long.parseLong(neighbor[0]);
                if (id1 < id2) {
                    node2.set(nbrs[i]);
                    context.write(node1, node2);
                }

            }
        }
    }

    public static class EdgeReader3 extends Mapper<Object, Text, LongWritable, LongWritable> {

        private LongWritable vertex = new LongWritable();
        private LongWritable neighbor = new LongWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer edgeIterator = new StringTokenizer(value.toString());
            while (edgeIterator.hasMoreTokens()) {
                String[] node1 = edgeIterator.nextToken().toString().split("," +
                        "");
                long id1 = Long.parseLong(node1[0]);
                long degree1 = Long.parseLong(node1[1]);

                String[] node2 = edgeIterator.nextToken().toString().split(",");
                long id2 = Long.parseLong(node2[0]);
                long degree2 = Long.parseLong(node2[1]);

                if (degree1 > degree2) {
                    vertex.set(id2);
                    neighbor.set(id1);
                    context.write(vertex, neighbor);
                } else {
                    vertex.set(id1);
                    neighbor.set(id2);
                    context.write(vertex, neighbor);
                }
            }
        }
    }

    public static class EdgeReducer extends Reducer<LongWritable, LongWritable, Text, Text> {
        private Text pair = new Text();

        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            int size = 0;

            long[] nbrs = new long[4096];

            Iterator<LongWritable> neighborIterator = values.iterator();

            while (neighborIterator.hasNext()) {
                if (nbrs.length == size) {
                    nbrs = Arrays.copyOf(nbrs, (int) (size * 1.5));
                }

                long neighbor = neighborIterator.next().get();

                nbrs[size++] = neighbor;

            }

            for (int i = 0; i < size; i++) {
                for (int j = i + 1; j < size; j++) {
                    String possibleEdge = Long.toString(nbrs[i]) + "," + Long.toString(nbrs[j]);
                    pair.set(possibleEdge);
                    context.write(new Text(key.toString()), pair);
                }
            }
        }
    }


    public static class TriangleMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text pairKey = new Text();
        private Text outValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String data = value.toString();
            String[] lines = data.split("\n");
            for( String line : lines ) {
                int index = line.indexOf(',');
                if( index != -1 ) {

                    String[] vPair = line.split("\t");
                    String v = vPair[0];
                    String pair = vPair[1];
                    pairKey.set(pair);
                    outValue.set(v);
                    context.write(pairKey, outValue);
                }
                else {

                    String[] edge = line.split("\t");
                    String v1 = edge[0];
                    String v2 = edge[1];
                    String pair = v1 + "," + v2;
                    pairKey.set(pair);
                    outValue.set("$");
                    context.write(pairKey, outValue);
                }
            }


        }
    }



    public static class TriangleReducer extends Reducer<Text, Text, LongWritable, LongWritable> {
        private LongWritable one = new LongWritable(1);

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long[] triangles = new long[4096];
            int size = 0;
            boolean iae = false;
            for (Text symbol : values) {
                if (symbol.toString().contentEquals("$")) {
                    iae = true;
                    continue;
                }

                long id = Long.parseLong(symbol.toString());

                if (triangles.length == size) {
                    triangles = Arrays.copyOf(triangles, (int) (size * 1.5));
                }

                triangles[size++] = id;
            }

            if (iae) {
                for (int i = 0; i < size; i++) {
                    context.write(new LongWritable(triangles[i]), one);
                }
            }
        }
    }

    public static class TriangleGetter extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

        private LongWritable vk = new LongWritable();
        private LongWritable one = new LongWritable(1);
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer vertexIterator = new StringTokenizer(value.toString());
            while (vertexIterator.hasMoreTokens()) {
                long vertex = Long.parseLong(vertexIterator.nextToken());
                vk.set(vertex);
                long num = Long.parseLong(vertexIterator.nextToken());
                context.write(one, new LongWritable(num));
            }
        }
    }


    public static class TriangleCounter extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            long count = 0;
            for (LongWritable val : values) {
                count += val.get();
            }

            context.write(new LongWritable(key.get()), new LongWritable(count));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task3");
        job.setJarByClass(Task3.class);
        job.setMapperClass(Task3.EdgeReadermapper.class);
        job.setReducerClass(Task3.Degree1Reducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "Triangle Count2");
        job2.setJarByClass(Task3.class);
        job2.setMapperClass(EdgeReader2.class);
        job2.setReducerClass(Degree2Reducer.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);

        Job job3 = Job.getInstance(conf, "Triangle Count3");
        job3.setJarByClass(Task3.class);
        job3.setMapperClass(EdgeReader3.class);
        job3.setReducerClass(EdgeReducer.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(args[2]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));
        job3.waitForCompletion(true);


        Job job4 = Job.getInstance(conf, "Triangle Count4");
        job4.setJarByClass(Task3.class);
        job4.setMapperClass(TriangleMapper.class);
        job4.setReducerClass(TriangleReducer.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(LongWritable.class);
        job4.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job4, new String(args[3]));
        FileOutputFormat.setOutputPath(job4, new Path(args[4]));
        job4.waitForCompletion(true);


        Job job5 = Job.getInstance(conf, "Triangle Count5");
        job5.setJarByClass(Task3.class);
        job5.setMapperClass(TriangleGetter.class);
        job5.setReducerClass(TriangleCounter.class);
        job5.setMapOutputKeyClass(LongWritable.class);
        job5.setMapOutputValueClass(LongWritable.class);
        job5.setOutputKeyClass(LongWritable.class);
        job5.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job5, new Path(args[4]));
        FileOutputFormat.setOutputPath(job5, new Path(args[5]));
        System.exit(job5.waitForCompletion(true) ? 0 : 1);

    }
}