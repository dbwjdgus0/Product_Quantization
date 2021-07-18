package writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class SimpleVecWritable implements Writable {

    public static int dim;
    public float[] vec = new float[dim];

    public SimpleVecWritable() {  }

    public SimpleVecWritable(float[] vec) { this.vec = vec; }

    public void set(float[] vec) { this.vec = vec; }

    @Override
    public void readFields(DataInput in) throws IOException {
        for(int i = 0; i < dim; i++) vec[i] = (in.readFloat());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for(float i: vec) out.writeFloat(i);
    }
}
