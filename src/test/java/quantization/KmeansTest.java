package quantization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class KmeansTest {

    public static void main(String[] args) throws Exception {

        int codeLen = 25;
        int dataSize = 1000000;

        Configuration conf = new Configuration();
        conf.setInt("dim",128);
        conf.setInt("codeLen", codeLen);
        conf.setInt("numVectors", dataSize);
        String inputPath = "data/sift_train.txt";
        String outputPath = "data/sift_128_kmeans";

        ToolRunner.run(conf, new Kmeans(), new String[]{inputPath, outputPath,
                Integer.toString(codeLen), Integer.toString(dataSize)});
    }
}
