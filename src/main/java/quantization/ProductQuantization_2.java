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
import writables.SimpleVecWritable;
import writables.VectorWritable;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

public class ProductQuantization_2 extends Configured implements Tool{

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInt("dim",128);
        conf.setInt("codeLen", 256);
        conf.setInt("numVectors", 1000000);
        conf.setInt("subDimNum", 8);
        conf.setInt("subDimLen", 128/8);
        conf.setInt("finished", 0);

        String inputPath = "data/sift_train.txt";
        String outputPath = inputPath + "_res";

        ToolRunner.run(conf, new ProductQuantization_2(), new String[]{inputPath, outputPath});

    }

    @Override
    public int run(String[] args) throws Exception {

        FileSystem fs = FileSystem.get(getConf());

        long t;
        int round=0;

        String inputPath = args[0];
        String outputPath = args[1];

        //String roundCentroidPath = "data/part-r-00000_res/part-r-00000";
        String roundCentroidPath = "data/sift_train.txt_init_cent/part-r-00000";
        String roundOutputPath = outputPath + (++round);

        long numChanges = -1;
        while(numChanges != 0){

            getConf().setStrings("roundCentroidPath", roundCentroidPath);

            t = System.currentTimeMillis();

            numChanges = runPQ(inputPath, roundOutputPath, round);

            //step, round, time(ms), mor, mob
            System.out.printf("round: %d\t%d\t%d\n",
                    round, (System.currentTimeMillis() - t), numChanges);

            fs.delete(new Path(outputPath + (round-1)), true);

            roundCentroidPath = outputPath + (round) + "/part-r-00000";
            roundOutputPath = outputPath + (++round);
        }

        //WriteResult.seqToText(roundCentroidPath, getConf());

        return 0;
    }

    public long runPQ(String inputPath, String outputPath, int round) throws Exception {

        Job job = Job.getInstance(getConf(), "ProductQuantization-"+round);
        job.setJarByClass(ProductQuantization_2.class);

        job.setMapperClass(PQMapper.class);
        job.setCombinerClass(PQCombiner.class);
        job.setReducerClass(PQReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(VectorWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(SimpleVecWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(false);

        for(int i =0; i < 8; i++) {
            System.out.println(KmeansCounters.values()[i] + " : " +
                    job.getCounters().findCounter(KmeansCounters.values()[i]).getValue());
            if (job.getCounters().findCounter(KmeansCounters.values()[i]).getValue() == 0) {
                int finished = getConf().getInt("finished", 0);
                finished += (int)Math.pow(2, i);
                getConf().setInt("finished", finished);
            }
        }

        return job.getCounters().findCounter(KmeansCounters.NUM_CHANGES).getValue();
    }

    public static class PQMapper extends Mapper<Object, Text, IntWritable, VectorWritable> {

        int dim, codeLen, subDimNum, subDimLen, finished;
        float[][] centroids = new float[codeLen*subDimNum][subDimLen];

        IntWritable ok = new IntWritable();
        VectorWritable oval = new VectorWritable();
        String centPath = new String();

        @Override
        protected void setup(Mapper<Object,Text, IntWritable, VectorWritable>.Context context) {

            Configuration conf = context.getConfiguration();
            dim = conf.getInt("dim",0);
            codeLen = conf.getInt("codeLen", 0);
            subDimNum = conf.getInt("subDimNum", 0);
            subDimLen = conf.getInt("subDimLen", 0);
            centPath = conf.get("roundCentroidPath", "");
            finished = conf.getInt("finished", 0);

            VectorWritable.dim = subDimLen;

            centroids = ReadCent.loadCentroids(conf, centPath);
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object,Text, IntWritable, VectorWritable>.Context context)
                throws IOException, InterruptedException {


            float[] vec = new float[dim];

            StringTokenizer st = new StringTokenizer(value.toString());
            //int id = Integer.parseInt(st.nextToken());
            int ind =0;
            while(st.hasMoreTokens()) vec[ind++] = Float.parseFloat(st.nextToken());

            for(int subdim = 0; subdim< subDimNum; subdim++)
            {

                ///if this subdimension is finished
//                if(((0x01 << subdim) & finished )!= 0) {
//
//                }

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

    public static class PQReducer extends Reducer<IntWritable, VectorWritable, IntWritable, SimpleVecWritable> {

        int subDimLen, codeLen, subDimNum, finished;
        float[][] centroids = new float[codeLen*subDimNum][subDimLen];
        String centPath = new String();

        IntWritable ok = new IntWritable();
        SimpleVecWritable oval = new SimpleVecWritable();

        @Override
        protected void setup(Reducer<IntWritable, VectorWritable, IntWritable, SimpleVecWritable>.Context context) {
            Configuration conf = context.getConfiguration();


            finished = conf.getInt("finished", 0);
            subDimLen = conf.getInt("subDimLen", 0);
            subDimNum = conf.getInt("subDimNum", 0);
            codeLen = conf.getInt("codeLen", 0);
            centPath = conf.get("roundCentroidPath", "");

            VectorWritable.dim = subDimLen;
            SimpleVecWritable.dim = subDimLen;

            centroids = ReadCent.loadCentroids(conf, centPath);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<VectorWritable> values,
                              Reducer<IntWritable, VectorWritable, IntWritable, SimpleVecWritable>.Context context)
                throws IOException, InterruptedException {

            float[] newCentroid = new float[subDimLen];
            int sumed = 0;

            //if this subdimension is finished
            if(((0x01 << (key.get()/256) ) & finished )!= 0) {
                for (VectorWritable value : values) {
                    System.arraycopy(value.vec, 0, newCentroid, 0, subDimLen);
                    break;
                }
                ok = key;
                oval.set(newCentroid);
                context.write(ok, oval);
                return;
            }


            for (VectorWritable value : values) {

                for (int i = 0; i < subDimLen; i++) {
                    newCentroid[i] += value.vec[i];
                }
                sumed += value.sumedNum;
            }

            for(int i = 0; i < subDimLen; i++) newCentroid[i] /= sumed;


            if (!Arrays.equals(centroids[key.get()], newCentroid)) {
                context.getCounter(KmeansCounters.NUM_CHANGES).increment(1);
                context.getCounter(KmeansCounters.values()[key.get()/256]).increment(1);
            }

            ok = key;
            oval.set(newCentroid);
            context.write(ok, oval);
        }

    }
}
