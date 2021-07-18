package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.StringTokenizer;

public class RandCentGen {

    public static void makeCent(int codeLen, int dataSize, String inPath, String outPath)
    {
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
                //st.nextToken();
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

            int cent_num = 0;
            while(cent_num++ < codeLen)
            {
                int val = r.nextInt(dataSize);
                while(selected.contains(val)) val = r.nextInt(dataSize);
                selected.add(val);

                String temp = "";
                for(float each: datas.get(val)) temp += Float.toString(each) + " ";

                key.set(cent_num);
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

    public static void makeCentBySubDim(int codeLen, int dataSize, int subDimNum, int subDimLen,  String inPath, String outPath)
    {
        HashSet<Integer> selected = new HashSet<>();
        Random r = new Random();
        //r.setSeed(0);

        ArrayList<float[]> datas = new ArrayList<>();

        File file = new File(inPath);
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null)
            {
                StringTokenizer st = new StringTokenizer(line);
                //st.nextToken();
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

            for( int i = 0; i < subDimNum; i++)
            {
                selected.clear();
                int cent_num = 0;

                while(cent_num < codeLen)
                {
                    int preFix = i * codeLen;

                    int val = r.nextInt(dataSize);
                    while(selected.contains(val)) val = r.nextInt(dataSize);
                    selected.add(val);

                    String temp = "";
                    for(int j = i * subDimLen ; j < (i+1) * subDimLen; j++ ) temp += Float.toString(datas.get(val)[j]) + " ";

                    key.set(preFix + cent_num);
                    value.set(temp);

                    writer.append(key, value);
                    cent_num++;
//                    System.out.println(key.get() + "\t" + value.toString());
//                    System.out.println(val + " selected" + " at " + i + "'s subdim");
                }

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

    public static void select_centroid_plus(int codeLen, int dataSize, int subDimNum, int subDimLen,  String inPath, String outPath)
    {
        return;
    }

//
//    public static void main(String[] args) {
//        makeCentBySubDim(4, 100, 4, "data/sift_head_100", "testout");
//        Configuration conf = new Configuration();
//        WriteResult.seqToText("testout" , conf);
//    }


}
