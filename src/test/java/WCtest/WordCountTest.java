package WCtest;

import org.apache.hadoop.util.ToolRunner;

public class WordCountTest {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new WordCount(), new String[] {"src/test/resources/testfile.txt"});
    }
}