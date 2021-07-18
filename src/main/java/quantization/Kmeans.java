package quantization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import writables.*;
import utils.*;

import java.io.*;
import java.util.*;

public class Kmeans extends Configured implements Tool {

    long mor, mob;

    public static void main(String[] args) throws Exception {

            if(args.length == 0) {
                int codeLen = 256;
                int dataSize = 1000000;

                Configuration conf = new Configuration();
                conf.setInt("dim", 128);
                conf.setInt("codeLen", codeLen);
                conf.setInt("numVectors", dataSize);
                //conf.setInt("numVectors", 1183514);
                String inputPath = "data/sift_train.txt";
                //String inputPath = "data/glove25d_train.txt";
                String outputPath = "data/sift_128_kmeans";

                ToolRunner.run(conf, new Kmeans(), new String[]{inputPath, outputPath,
                        Integer.toString(codeLen), Integer.toString(dataSize)});
            }
            else
            {
                ToolRunner.run(new Kmeans(), args);
            }
    }

    @Override
    public int run(String[] args) throws Exception {

        FileSystem fs = FileSystem.get(getConf());

        long t;
        int round=0;

        String inputPath = args[0];
        String outputPath = args[1];
        String randomCentPath = outputPath + ".centroids_" + (round);

        t = System.currentTimeMillis();
        RandCentGen.makeCent(Integer.parseInt(args[2]), Integer.parseInt(args[3]), inputPath, randomCentPath);
        System.out.println("randCentGen: " + (System.currentTimeMillis() - t));


        String roundCentroidPath = outputPath + ".centroids_" + (round);
        String roundOutputPath = outputPath + ".centroids_" + (++round);

        long numChanges = -1;
        while(numChanges != 0){


            getConf().setStrings("roundCendroidPath", roundCentroidPath);

            t = System.currentTimeMillis();

            numChanges = runKmeans(inputPath, roundOutputPath, round);

            //step, round, time(ms), mor, mob
            System.out.printf("round: %d\t%d\t%d\t%d\t%d\n",
                    round, (System.currentTimeMillis() - t), mor, mob, numChanges);

            fs.delete(new Path(outputPath + ".centroids_" + (round-1)), true);

            roundCentroidPath = outputPath + ".centroids_" + (round) + "/part-r-00000";
            roundOutputPath = outputPath + ".centroids_" + (++round);
        }

        WriteResult.seqToText(roundCentroidPath, getConf());

        return 0;
    }

    public long runKmeans(String inputPath, String outputPath, int round) throws Exception {

        Job job = Job.getInstance(getConf(), "Kmeans-"+round);
        job.setJarByClass(Kmeans.class);

        job.setMapperClass(KmeansMapper.class);
        job.setCombinerClass(KmeansCombiner.class);
        job.setReducerClass(KmeansReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(VectorWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(false);

        mor = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        mob = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();

        return job.getCounters().findCounter(KmeansCounters.NUM_CHANGES).getValue();
    }



    public static class KmeansMapper extends Mapper<Object, Text, IntWritable, VectorWritable> {

        public int dim;
        int codeLen;

        IntWritable ok = new IntWritable();
        VectorWritable oval = new VectorWritable();

        float[][] centroids = new float[codeLen][dim];

        String centPath = new String();

        @Override
        protected void setup(Mapper<Object,Text, IntWritable, VectorWritable>.Context context) {
            Configuration conf = context.getConfiguration();
            dim = conf.getInt("dim",0);
            codeLen = conf.getInt("codeLen", 0);
            centPath = conf.get("roundCendroidPath", "");
            VectorWritable.dim = dim;

            centroids = ReadCent.loadCentroids(conf, centPath);
        }

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object,Text, IntWritable, VectorWritable>.Context context) throws IOException, InterruptedException {

            float[] vec = new float[dim];
            int closest_cent = 0;

            StringTokenizer st = new StringTokenizer(value.toString());

            //int id = Integer.parseInt(st.nextToken());
            int ind =0;
            while(st.hasMoreTokens()) vec[ind++] = Float.parseFloat(st.nextToken());

            float min_dist = Float.POSITIVE_INFINITY;

            for(int i = 0 ; i < codeLen ; i++)
            {
                float dist = Distances.l2(vec, centroids[i]);
                if (dist < min_dist)
                {
                    min_dist = dist;
                    closest_cent = i;
                }
            }

            ok.set(closest_cent);
            oval.set(vec);
            context.write(ok, oval);
        }
    }

    public static class KmeansCombiner extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>
    {
        IntWritable ok = new IntWritable();
        VectorWritable oval = new VectorWritable();
        int dim;

        @Override
        protected void setup(Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context) {
            Configuration conf = context.getConfiguration();
            dim = conf.getInt("dim",0);
            VectorWritable.dim = dim;

        }

        @Override
        protected void reduce(IntWritable key, Iterable<VectorWritable> values,
                              Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context)
                throws IOException, InterruptedException {

            int cnt = 0;
            float[] sumVector = new float[dim];
            for(VectorWritable value: values)
            {
                float[] temp = value.vec;
                for(int i = 0; i < dim; i++) sumVector[i]+=temp[i];
                cnt++;
            }

            oval.set(sumVector);
            oval.sumedNum = cnt;

            ok.set(key.get());

            context.write(ok, oval);
        }
    }

    public static class KmeansReducer extends Reducer<IntWritable, VectorWritable, IntWritable, Text>
    {
        IntWritable ok = new IntWritable();
        Text oval = new Text();
        int dim, codeLen;
        String centPath = new String();

        float[][] centroids = new float[codeLen][dim];


        @Override
        protected void setup(Reducer<IntWritable, VectorWritable, IntWritable, Text>.Context context) {
            Configuration conf = context.getConfiguration();
            dim = conf.getInt("dim",0);
            centPath = conf.get("roundCendroidPath", "");
            codeLen = conf.getInt("codeLen", 0);
            VectorWritable.dim = dim;
            centroids = ReadCent.loadCentroids(conf, centPath);
        }


        @Override
        protected void reduce(IntWritable key, Iterable<VectorWritable> values,
                              Reducer<IntWritable, VectorWritable, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {

            float[] newCentroid = new float[dim];

            int sumed = 0;

            for(VectorWritable value: values)
            {
                for(int i = 0 ; i < dim ; i++) newCentroid[i]+=value.vec[i];
                sumed += value.sumedNum;
            }


            for(int i =0; i < dim; i++) newCentroid[i] /= sumed;

            if(!Arrays.equals(centroids[key.get()], newCentroid))
                context.getCounter(KmeansCounters.NUM_CHANGES).increment(1);

            String outString = "";
            for(float each: newCentroid) outString+= Float.toString(each) + " ";

            ok.set(key.get());
            oval.set(outString);
            context.write(ok, oval);
        }
    }




}
