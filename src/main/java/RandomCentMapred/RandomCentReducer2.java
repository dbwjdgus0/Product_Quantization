package RandomCentMapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import writables.CentroidWritable;
import writables.DimFloatPairWritable;

import java.io.IOException;
import java.util.Random;

public class RandomCentReducer2 extends Reducer<IntWritable, DimFloatPairWritable, IntWritable, Text> {

        int dim, cents;

        IntWritable ok = new IntWritable();
        //CentroidWritable oval = new CentroidWritable();
        Text oval = new Text();
    @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            dim = conf.getInt("dim",0);
            cents = conf.getInt("codeLen", 0);
            CentroidWritable.dim = dim;
        }

        @Override
        protected void reduce(IntWritable key, Iterable<DimFloatPairWritable> values, Context context) throws IOException, InterruptedException {

            //float[] centroidVec = new float[dim];
            String centroidVec = "";
            Random r = new Random();
            for(DimFloatPairWritable val: values)
            {
                //centroidVec[val.dim] = r.nextFloat() * (val.max - val.min) + val.min;
                centroidVec +=  Float.toString(r.nextFloat() * (val.max - val.min) + val.min) + " ";
            }

            ok = key;
            //oval.set((byte)key.get(), centroidVec);
            oval.set(centroidVec);
            context.write(ok, oval);
        }
}

