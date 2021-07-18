package quantization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import writables.VectorWritable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class test {
    public static void main(String[] args) {


        int finished = 4;

        for(int subdim = 0; subdim< 8; subdim++)
        {
            if(((0x01 << subdim) & finished )!= 0) { continue; }
            else{System.out.println(subdim);}
        }
    }

    public static void reader()
    {
        Configuration conf = new Configuration();
        Path path = new Path("data/testout");
        IntWritable key = new IntWritable();
        Text val = new Text();

        try {
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
            while(reader.next(key, val))
            {
                System.out.println(key + "\t" + val);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static void randCentGen(int codeLen, int dataSize, String inPath, String outPath){

        HashSet<Integer> selected = new HashSet<>();
        Random r = new Random();
        r.setSeed(0);

        ArrayList<float[]> datas = new ArrayList<>();

        File file = new File(inPath);
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null)
            {
                StringTokenizer st = new StringTokenizer(line);
                st.nextToken();
                float[] vec = new float[st.countTokens()];
                int ind = 0;
                while(st.hasMoreTokens()) vec[ind++] = Float.parseFloat(st.nextToken());

                datas.add(vec);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        SequenceFile.Writer writer = null;

        try{
            Path path = new Path(outPath);
            IntWritable key = new IntWritable();
            Text value = new Text();

            Configuration conf = new Configuration();

            writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(key.getClass()),
                    ArrayFile.Writer.valueClass(value.getClass()));

            int iter = 0;
            while(iter++ < codeLen)
            {
                int val = r.nextInt(dataSize);
                while(selected.contains(val)) val = r.nextInt(dataSize);
                selected.add(val);

                key.set(val);
                String temp = "";
                for(float each: datas.get(val)) temp += Float.toString(each) + " ";

                value.set(temp);

                writer.append(key, value);
            }

            writer.close();

        }
        catch (IOException e){
            e.printStackTrace();
        }
        finally {
            IOUtils.closeStream(writer);
        }


    }

}
