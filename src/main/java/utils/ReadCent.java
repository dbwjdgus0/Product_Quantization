package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import writables.SimpleVecWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class ReadCent {

    public static float[][] loadCentroids(Configuration conf, String centPath)
    {
        int subDimNum = conf.getInt("subDimNum", 8);
        int subDimLen = conf.getInt("subDimlen", 16);
        int codeLen = conf.getInt("codeLen", 256);
        float[][] centroids = new float[codeLen*subDimNum][subDimLen];

        Path path = new Path(centPath);
        IntWritable key = new IntWritable();
        SimpleVecWritable.dim = subDimLen;
        SimpleVecWritable val = new SimpleVecWritable();

        try {
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
            int idx= 0;
            while(reader.next(key, val)) {
                System.arraycopy(val.vec, 0, centroids[idx++],0, subDimLen );
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        return centroids;
    }

    public static void main(String[] args) {
        long t = System.currentTimeMillis();
        float[][] ret = loadCentroids(new Configuration(), "data/part-r-00000_res/part-r-00000");
        System.out.println(System.currentTimeMillis() - t);

        System.out.println(ret.length);

        for(int i =0; i < 2048; i++){
            System.out.print(i + ": ");
            for(int j =0; j<16; j++){
                System.out.print(ret[i][j] + " ");
            }
            System.out.println();
        }
    }
}
