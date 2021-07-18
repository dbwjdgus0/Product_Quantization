package writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;


public class VectorDistWritable implements Writable {

    public static int dim;

    public float[] vec = new float[dim];
    public float dist;

    public VectorDistWritable() {
        this.dist = 0;
    }

    public VectorDistWritable(float[] vec, float dist) {
        this.vec = vec;
        this.dist = dist;
    }

    public void set(float[] vec, float dist) {
        this.vec = vec;
        this.dist = dist;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        dist = in.readFloat();
        for(int i = 0; i < dim; i++) vec[i] = (in.readFloat());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(dist);
        for(float i: vec) out.writeFloat(i);
    }

    @Override
    public String toString() {
        String ret = "";
        for(float each: vec)
        {
            ret += Float.toString(each) + " ";
        }
        return ret;
    }
}
