package writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class VectorWritable implements Writable {

    public static int dim;

    public float[] vec = new float[dim];
    public int sumedNum;

    public VectorWritable() {
        sumedNum = 1;
    }

    public VectorWritable(float[] vec) {
        this.vec = vec;
        sumedNum = 1;
    }

    public void set(float[] vec) {
        this.vec = vec;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sumedNum = in.readInt();
        for(int i = 0; i < dim; i++) vec[i] = (in.readFloat());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(sumedNum);
        for(float i: vec) out.writeFloat(i);
    }
}
