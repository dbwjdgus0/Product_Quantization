package writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class CentroidWritable implements Writable{

    public static int dim=0;

    byte code;
    float[] vec = new float[dim];

    public void set(byte code, float[] vec) {
        this.code = code;
        this.vec = vec;
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        code = in.readByte();
        for(int i = 0; i < dim; i++) vec[i] = in.readFloat();
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeByte(code);
        for(float i: vec) out.writeFloat(i);
    }
}



