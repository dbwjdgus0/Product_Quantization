package quantization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class PQTest {

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        conf.setInt("dim",2);
        //conf.setInt("codeLen", 256);
        conf.setInt("codeLen", 16);
        //conf.setInt("numVectors", 1000000);
        conf.setInt("numVectors", 4500);

        conf.setInt("subDimNum", 1);
        conf.setInt("subDimLen", 2);
        //String inputPath = "data/sift_train.txt";
        //String inputPath = "data/sift_head_100";
        String inputPath = "data/2dim_test_2.txt";

        String outputPath = inputPath + "_res";

        ToolRunner.run(conf, new Centroid_select(), new String[]{inputPath, outputPath});

        //ToolRunner.run(conf, new ProductQuantization(), new String[]{inputPath, outputPath});
    }
}
