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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import utils.Distances;
import utils.RandCentGen;
import utils.ReadCent;
import utils.WriteResult;
import writables.VectorWritable;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

public class ProductQuantization extends Configured implements Tool{

    long mor, mob;



    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ProductQuantization(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        FileSystem fs = FileSystem.get(getConf());

        long t;
        int round=0;

        String inputPath = args[0];
        String outputPath = args[1];

        String roundCentroidPath = "data/sift_train.txt_init_cent/part-r-00000";
        String roundOutputPath = outputPath + (++round);

        long numChanges = -1;
        while(numChanges != 0){

            getConf().setStrings("roundCentroidPath", roundCentroidPath);

            t = System.currentTimeMillis();

            numChanges = runKmeans(inputPath, roundOutputPath, round);

            //step, round, time(ms), mor, mob
            System.out.printf("round: %d\t%d\t%d\t%d\t%d\n",
                    round, (System.currentTimeMillis() - t), mor, mob, numChanges);

            fs.delete(new Path(outputPath + (round-1)), true);

            roundCentroidPath = outputPath + (round) + "/part-r-00000";
            roundOutputPath = outputPath + (++round);

        }

        WriteResult.seqToText(roundCentroidPath, getConf());

        return 0;
    }

    public long runKmeans(String inputPath, String outputPath, int round) throws Exception {

        Job job = Job.getInstance(getConf(), "ProductQuantization-"+round);
        job.setJarByClass(ProductQuantization.class);

        job.setMapperClass(PQMapper.class);
        job.setCombinerClass(PQCombiner.class);
        job.setReducerClass(PQReducer.class);

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


        for(int i =0; i < 8; i++) {

            System.out.println(KmeansCounters.values()[i] + " : " + job.getCounters().findCounter(KmeansCounters.values()[i]).getValue());
            if( job.getCounters().findCounter(KmeansCounters.values()[i]).getValue() == 0){

            }

        }

        return job.getCounters().findCounter(KmeansCounters.NUM_CHANGES).getValue();
    }

    public static class PQMapper extends Mapper<Object, Text, IntWritable, VectorWritable> {

        int dim, codeLen, subDimNum, subDimLen;

        IntWritable ok = new IntWritable();
        VectorWritable oval = new VectorWritable();
        float[][] centroids = new float[codeLen*subDimNum][subDimLen];
        String centPath = new String();

        @Override
        protected void setup(Mapper<Object,Text, IntWritable, VectorWritable>.Context context) {

            Configuration conf = context.getConfiguration();
            dim = conf.getInt("dim",0);
            codeLen = conf.getInt("codeLen", 0);
            subDimNum = conf.getInt("subDimNum", 0);
            subDimLen = conf.getInt("subDimLen", 0);
            centPath = conf.get("roundCentroidPath", "");
            VectorWritable.dim = subDimLen;

            centroids = ReadCent.loadCentroids(conf, centPath);

        }

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object,Text, IntWritable, VectorWritable>.Context context) throws IOException, InterruptedException {

            float[] vec = new float[dim];

            StringTokenizer st = new StringTokenizer(value.toString());
            //int id = Integer.parseInt(st.nextToken());
            int ind =0;
            while(st.hasMoreTokens()) vec[ind++] = Float.parseFloat(st.nextToken());

            for(int subdim = 0; subdim< subDimNum; subdim++)
            {
                float[] subVec = Arrays.copyOfRange(vec, subdim * subDimLen, (subdim+ 1) * subDimLen);

                float min_dist = Float.POSITIVE_INFINITY;
                int closest_cent = 0;

                for(int cent_idx = subdim * codeLen ; cent_idx < (subdim+ 1) * codeLen; cent_idx++)
                {
                    float dist = Distances.l2(subVec, centroids[cent_idx]);
                    if (dist < min_dist)
                    {
                        min_dist = dist;
                        closest_cent = cent_idx;
                    }
                }
                ok.set(closest_cent);
                oval.set(subVec);
                context.write(ok, oval);
            }

        }
    }

    public static class PQCombiner extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>
    {

        IntWritable ok = new IntWritable();
        VectorWritable oval = new VectorWritable();
        int subDimLen;

        @Override
        protected void setup(Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context) {
            Configuration conf = context.getConfiguration();
            subDimLen = conf.getInt("subDimLen",0);
            VectorWritable.dim = subDimLen;

        }

        @Override
        protected void reduce(IntWritable key, Iterable<VectorWritable> values,
                              Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context)
                throws IOException, InterruptedException {

            int cnt = 0;
            float[] sumVector = new float[subDimLen];
            for(VectorWritable value: values)
            {
                float[] temp = value.vec;
                for(int i = 0; i < subDimLen; i++) sumVector[i]+=temp[i];
                cnt++;
            }

            oval.set(sumVector);
            oval.sumedNum = cnt;

            ok.set(key.get());

            context.write(ok, oval);
        }
    }

    public static class PQReducer extends Reducer<IntWritable, VectorWritable, IntWritable, Text> {

        IntWritable ok = new IntWritable();
        Text oval = new Text();
        int subDimLen, subDimNum, codeLen;
        String centPath = new String();

        float[][] centroids = new float[codeLen*subDimNum][subDimLen];


        @Override
        protected void setup(Reducer<IntWritable, VectorWritable, IntWritable, Text>.Context context) {
            Configuration conf = context.getConfiguration();
            subDimLen = conf.getInt("subDimLen", 0);
            subDimNum = conf.getInt("subDimNum", 0);
            codeLen = conf.getInt("codeLen", 0);
            centPath = conf.get("roundCentroidPath", "");
            VectorWritable.dim = subDimLen;

            centroids = ReadCent.loadCentroids(conf, centPath);
        }


        @Override
        protected void reduce(IntWritable key, Iterable<VectorWritable> values,
                              Reducer<IntWritable, VectorWritable, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {

            float[] newCentroid = new float[subDimLen];
            int sumed = 0;

            for (VectorWritable value : values) {
                for (int i = 0; i < subDimLen; i++) newCentroid[i] += value.vec[i];
                sumed += value.sumedNum;
            }

            for (int i = 0; i < subDimLen; i++) newCentroid[i] /= sumed;

            if (!Arrays.equals(centroids[key.get()], newCentroid)) {
                context.getCounter(KmeansCounters.NUM_CHANGES).increment(1);
                context.getCounter(KmeansCounters.values()[key.get()/256]).increment(1);
            }

            String outString = "";
            for (float each : newCentroid) outString += Float.toString(each) + " ";

            ok.set(key.get());
            oval.set(outString);
            context.write(ok, oval);
        }

    }
}
