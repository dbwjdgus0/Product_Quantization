package writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DimFloatPairWritable implements Writable {

    public int dim;
    public float min;
    public float max;

    public void set(int dim, float min, float max){
        this.dim = dim;
        this.min = min;
        this.max = max;
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        dim = in.readInt();
        min = in.readFloat();
        max = in.readFloat();
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeInt(dim);
        out.writeFloat(min);
        out.writeFloat(max);
    }
}
