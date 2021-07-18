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
import writables.VectorDistWritable;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.StringTokenizer;


public class Centroid_select extends Configured implements Tool {

    long mor, mob;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Centroid_select(), args);
    }

    @Override
    public int run(String[] args) throws Exception {


        FileSystem fs = FileSystem.get(getConf());
        String inputPath = args[0];
        String outputPath = args[1];

        long t;
        int round=1;
        int codeLen = getConf().getInt("codeLen", 0);
        String roundCentroidPath = outputPath+ "0";
        String roundOutputPath = outputPath + "1";

        RandCentGen.makeCentBySubDim(1, getConf().getInt("numVectors", 0),
                getConf().getInt("subDimNum",0), getConf().getInt("subDimLen", 0),inputPath, roundCentroidPath);


        while(round < codeLen){

            getConf().setInt("round", round);
            getConf().setStrings("roundCentroidPath", roundCentroidPath);

            t = System.currentTimeMillis();
            runCSelect(inputPath, roundOutputPath, round);

            //step, round, time(ms), mor, mob
            System.out.printf("round: %d\t%d\t%d\t%d\n",
                    round, (System.currentTimeMillis() - t), mor, mob);

            fs.delete(new Path(outputPath + (round-1)), true);

            roundCentroidPath = outputPath + (round) + "/part-r-00000";
            roundOutputPath = outputPath + (++round);

            if(round == codeLen-1) {
                roundOutputPath = outputPath;
            }

        }

        return 0;
    }

    public long runCSelect(String inputPath, String outputPath, int round) throws Exception {

        Job job = Job.getInstance(getConf(), "Centroid Select - "+round);
        job.setJarByClass(Centroid_select.class);

        job.setMapperClass(CSelectMapper.class);
        job.setReducerClass(CSelectReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(VectorDistWritable.class);

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

    public static class CSelectMapper extends Mapper<Object, Text, IntWritable, VectorDistWritable> {

        int dim, codeLen, subDimNum, subDimLen, round;

        IntWritable ok = new IntWritable();
        VectorDistWritable oval = new VectorDistWritable();
        float[][] centroids = new float[codeLen*subDimNum][subDimLen];
        String centPath = new String();

        @Override
        protected void setup(Mapper<Object, Text, IntWritable, VectorDistWritable>.Context context) {

            Configuration conf = context.getConfiguration();
            dim = conf.getInt("dim", 0);
            codeLen = conf.getInt("codeLen", 0);
            subDimNum = conf.getInt("subDimNum", 0);
            subDimLen = conf.getInt("subDimLen", 0);
            round = conf.getInt("round", 0);
            centPath = conf.get("roundCentroidPath", "");
            VectorDistWritable.dim = subDimLen;

            centroids = ReadCent.loadCentroids(conf, centPath);
        }

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, IntWritable, VectorDistWritable>.Context context) throws IOException, InterruptedException {

            float[][] vecs = new float[subDimNum][subDimLen];

            StringTokenizer st = new StringTokenizer(value.toString());

            for(int i = 0; i < subDimNum; i++) {
                for(int j =0; j < subDimLen; j++) {vecs[i][j] = Float.parseFloat(st.nextToken()); }
            }

            for(int subDim = 0; subDim < subDimNum ; subDim++)
            {
                ArrayList<float[]> cents_for_this_subDim = new ArrayList<>();

                int base = subDim * round;
                for(int c = 0; c < round; c++) cents_for_this_subDim.add(centroids[base+c]);

                float min = Float.POSITIVE_INFINITY;
                float[] minVec = new float[subDimLen];

                for(float[] cent: cents_for_this_subDim)
                {
                    float dist = Distances.l2(vecs[subDim], cent);
                    if(dist< min)
                    {
                        min = dist;
                        System.arraycopy(vecs[subDim], 0, minVec, 0, subDimLen);
                    }
                }

                ok.set(subDim);
                oval.set(minVec, min);
                context.write(ok, oval);
            }
        }

    }


    public static class CSelectReducer extends Reducer<IntWritable, VectorDistWritable, IntWritable, SimpleVecWritable> {

        IntWritable ok = new IntWritable();
        SimpleVecWritable oval = new SimpleVecWritable();
        int subDimLen, codeLen, subDimNum, round;
        String centPath = new String();
        float[][] centroids = new float[codeLen*subDimNum][subDimLen];

        @Override
        protected void setup(Reducer<IntWritable, VectorDistWritable, IntWritable, SimpleVecWritable>.Context context) {
            Configuration conf = context.getConfiguration();
            subDimLen = conf.getInt("subDimLen", 0);
            subDimNum = conf.getInt("subDimNum", 0);
            codeLen = conf.getInt("codeLen", 0);
            centPath = conf.get("roundCentroidPath", "");
            round = conf.getInt("round", 0);
            VectorDistWritable.dim = subDimLen;

            centroids = ReadCent.loadCentroids(conf, centPath);
        }


        @Override
        protected void reduce(IntWritable key, Iterable<VectorDistWritable> values,
                              Reducer<IntWritable, VectorDistWritable, IntWritable, SimpleVecWritable>.Context context)
                throws IOException, InterruptedException {

            int base = key.get() * round;
            float max = -1;
            float[] maxVec = new float[subDimLen];

            for(VectorDistWritable value : values) {

                if(value.dist > max)
                {
                    max = value.dist;
                    System.arraycopy(value.vec, 0, maxVec, 0, subDimLen);
                }
            }

            for(int i = base; i < base + round; i++)
            {
                ok.set(i+ key.get());
                oval.set(centroids[i]);
                context.write(ok, oval);
            }

            ok.set(base+round+key.get());
            oval.set(maxVec);

            context.write(ok, oval);


        }

    }
}
