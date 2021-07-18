package quantization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import writables.SimpleVecWritable;

import java.io.IOException;
import java.util.StringTokenizer;

public class TextToSeq extends Configured implements Tool{

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String inputPath = "data/sift_train.txt_init_cent_text/part-r-00000";
        String outputPath = inputPath + "_res";
        ToolRunner.run(conf, new TextToSeq(), new String[]{inputPath, outputPath});
    }

    @Override
    public int run(String[] args) throws Exception {


        String inputPath = args[0];
        String outputPath = args[1];

        runLM(inputPath, outputPath);

        return 0;
    }

    public long runLM(String inputPath, String outputPath) throws Exception {

        Job job = Job.getInstance(getConf(), "doNothing");
        job.setJarByClass(TextToSeq.class);

        job.setMapperClass(DoNotMapper.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SimpleVecWritable.class);
        //job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(SimpleVecWritable.class);
        //job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(false);

        return 0;
    }

    public static class DoNotMapper extends Mapper<Object, Text, IntWritable, SimpleVecWritable> {

        IntWritable ok = new IntWritable();
        SimpleVecWritable oval = new SimpleVecWritable();
        //Text oval = new Text();

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object,Text, IntWritable, SimpleVecWritable>.Context context) throws IOException, InterruptedException {
            SimpleVecWritable.dim = 16;

            StringTokenizer st = new StringTokenizer(value.toString());
            int k = Integer.parseInt(st.nextToken());
            float[] v= new float[16];
            int i = 0;
            while(st.hasMoreTokens()){ v[i++] = Float.parseFloat(st.nextToken()); }

            ok.set(k);
            oval.set(v);
            context.write(ok, oval);
        }
    }




}
