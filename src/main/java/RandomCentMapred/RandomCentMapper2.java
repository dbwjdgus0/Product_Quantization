package RandomCentMapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import writables.DimFloatPairWritable;

import java.io.IOException;

public class RandomCentMapper2 extends Mapper<IntWritable, DimFloatPairWritable, IntWritable, DimFloatPairWritable> {

    IntWritable ok = new IntWritable();
    DimFloatPairWritable oval = new DimFloatPairWritable();

    int cents;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        cents = conf.getInt("codeLen", 0);
    }

    @Override
    protected void map(IntWritable key, DimFloatPairWritable value, Context context) throws IOException, InterruptedException {
        for(int i = 0 ; i < cents; i++)
        {
            ok.set(i);
            oval = value;
            context.write(ok, oval);
        }
    }
}