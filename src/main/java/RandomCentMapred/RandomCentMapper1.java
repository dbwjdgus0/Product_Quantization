package RandomCentMapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class RandomCentMapper1 extends Mapper<Object, Text, IntWritable, FloatWritable> {

    public int dim;
    IntWritable ok = new IntWritable();
    FloatWritable oval = new FloatWritable();

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        dim = conf.getInt("dim",0);

    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer st = new StringTokenizer(value.toString());
        int id = Integer.parseInt(st.nextToken());
        float val;

        for(int i=0; i<dim; i++){
            ok.set(i);
            val = Float.parseFloat(st.nextToken());
            oval.set(val);
            context.write(ok, oval);
        }
    }
}
