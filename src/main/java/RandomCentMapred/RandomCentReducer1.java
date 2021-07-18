package RandomCentMapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import writables.DimFloatPairWritable;

import java.io.IOException;

public class RandomCentReducer1 extends Reducer<IntWritable, FloatWritable, IntWritable, DimFloatPairWritable> {

    public int dim;
    IntWritable ok = new IntWritable();
    DimFloatPairWritable oval = new DimFloatPairWritable();
    Text test = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        dim = conf.getInt("dim",0);
    }


    @Override
    protected void reduce(IntWritable d, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

        float min = Float.POSITIVE_INFINITY;
        float max = Float.NEGATIVE_INFINITY;
        for(FloatWritable val : values){
            if (val.get() < min) min = val.get();
            else if (val.get() > max) max = val.get();
        }
        ok = d;
        oval.set(d.get(),min, max);
        context.write(ok, oval);

    }
}
