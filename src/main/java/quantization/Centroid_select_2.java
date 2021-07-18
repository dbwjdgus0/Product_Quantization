package quantization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import utils.Distances;
import utils.RandCentGen;
import utils.ReadCent;
import utils.WriteResult;
import writables.SimpleVecWritable;
import writables.VectorDistWritable;
import writables.VectorWritable;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;

public class Centroid_select_2 extends Configured implements Tool {

    long mor, mob;
    Logger logger = Logger.getLogger(getClass());

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInt("dim",128);
        conf.setInt("codeLen", 256);
        //conf.setInt("codeLen", 32);
        conf.setInt("numVectors", 1000000);
        //conf.setInt("numVectors", 100);
        conf.setInt("subDimNum",  8);
        conf.setInt("subDimLen", 16);

        String inputPath = "data/sift_train.txt";
        //String inputPath = "data/2dim_test_2.txt";
        //String inputPath = "data/sift_head_100";
        String outputPath = inputPath + "_init_cent_3";

        ToolRunner.run(conf, new Centroid_select_2(), new String[]{inputPath, outputPath});
    }

    @Override
    public int run(String[] args) throws Exception {

        String inputPath = args[0];
        String outputPath = args[1];

        long t;

        t = System.currentTimeMillis();
        runCSelect2(inputPath, outputPath);

        //step, round, time(ms), mor, mob
        System.out.printf("round: %d\t%d\t%d\n", (System.currentTimeMillis() - t), mor, mob);

        return 0;
    }

    public long runCSelect2(String inputPath, String outputPath) throws Exception {
        int timeout = getConf().getInt("mapred.task.timeout", 0);
        logger.info("timeout:   " + timeout);

        Job job = Job.getInstance(getConf(), "Centroid Select - 2");
        job.setJarByClass(Centroid_select_2.class);

        job.setMapperClass(CSelectMapper2.class);
        job.setReducerClass(CSelectReducer2.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SimpleVecWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(false);

        mor = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        mob = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();

        return 0;
    }

    public static class CSelectMapper2 extends Mapper<Object, Text, IntWritable, SimpleVecWritable> {

        int dim, codeLen, subDimNum, subDimLen, round;

        IntWritable okey = new IntWritable();
        SimpleVecWritable oval = new SimpleVecWritable();

        @Override
        protected void setup(Mapper<Object, Text, IntWritable, SimpleVecWritable>.Context context) {

            Configuration conf = context.getConfiguration();
            dim = conf.getInt("dim", 0);
            codeLen = conf.getInt("codeLen", 0);
            subDimNum = conf.getInt("subDimNum", 0);
            subDimLen = conf.getInt("subDimLen", 0);
            round = conf.getInt("round", 0);
            SimpleVecWritable.dim = subDimLen;

        }

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, IntWritable, SimpleVecWritable>.Context context) throws IOException, InterruptedException {

            StringTokenizer st = new StringTokenizer(value.toString());

            for(int s = 0; s < subDimNum; s++){
                float[] subVec = new float[subDimLen];
                for(int i = 0; i < subDimLen ; i++) subVec[i] = Float.parseFloat(st.nextToken());
                okey.set(s);
                oval.set(subVec);
                context.write(okey, oval);
            }
        }
    }


    public static class CSelectReducer2 extends Reducer<IntWritable, SimpleVecWritable, IntWritable, Text> {

        IntWritable ok = new IntWritable();
        Text oval = new Text();

        int subdimLen, subDimNum, numVectors, codeLen;

        @Override
        protected void setup(Reducer<IntWritable, SimpleVecWritable, IntWritable, Text>.Context context) {
            Configuration conf = context.getConfiguration();
            subdimLen = conf.getInt("subDimLen", 0);
            subDimNum = conf.getInt("subDimNum", 0);
            numVectors = conf.getInt("numVectors", 0);
            codeLen = conf.getInt("codeLen", 0);
            SimpleVecWritable.dim = subdimLen;
        }


        @Override
        protected void reduce(IntWritable key, Iterable<SimpleVecWritable> values,
                              Reducer<IntWritable, SimpleVecWritable, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {

            System.out.println("reducer " + key.get() + " operated");
            System.out.println(System.currentTimeMillis());

            ArrayList<Integer> centroids = new ArrayList<>();
            float[][] vectors = new float[numVectors][subdimLen];
            int index = 0;
            for (SimpleVecWritable val : values) { System.arraycopy(val.vec, 0, vectors[index++], 0, subdimLen); }

            Random r = new Random();
            centroids.add(r.nextInt(numVectors));

            for (int i = 1; i < codeLen; i++) {

                float[] closest_dist = new float[numVectors];

                for (int v = 0; v < numVectors; v++) {

                    float minDist = Float.POSITIVE_INFINITY;
                    for (int c : centroids) {
                        float dist = Distances.l2(vectors[v], vectors[c]);
                        if (dist < minDist) minDist = dist;
                    }
                    closest_dist[v] = minDist;
                }

                float maxDist = Float.NEGATIVE_INFINITY;
                int nextCent = -1;

                for (int idx = 0; idx < numVectors; idx++) {

                    if (closest_dist[idx] > maxDist) {
                        nextCent = idx;
                        maxDist = closest_dist[idx];
                    }
                }
                centroids.add(nextCent);
            }

            for (int i = 0; i < codeLen; i++){
                ok.set(key.get()*codeLen + i);
                String outString = "";
                for(int j =0 ; j < subdimLen; j++) outString += Float.toString(vectors[centroids.get(i)][j]) + " ";
                oval.set(outString);
                context.write(ok, oval);
            }
        }
    }
}
