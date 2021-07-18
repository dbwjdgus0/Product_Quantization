package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import writables.SimpleVecWritable;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

public class WriteResult {

    public static  void seqToText(String roundOutputPath, Configuration conf)
    {
        int dim = conf.getInt("subDimLen", 0);
        SimpleVecWritable.dim = dim;
        Path path = new Path(roundOutputPath);
        IntWritable key = new IntWritable();
        SimpleVecWritable val = new SimpleVecWritable();

        File file = new File(roundOutputPath + ".txt");
        try{

            BufferedWriter writer = new BufferedWriter(new FileWriter(file));
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
            while(reader.next(key, val))
            {
                float[] v = new float[dim];
                System.arraycopy(val.vec, 0, v, 0, dim);

                writer.write(key.get() + "\t");
                for(float each:v) {
                    writer.write(Float.toString(each) + " ");
                }
                writer.write("\n");
            }

            writer.close();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInt("codeLen", 256);
        conf.setInt("subDimNum", 8);
        conf.setInt("subDimLen", 16);
        seqToText( "data/part-r-00000_res/part-r-00000", conf);
    }

}
