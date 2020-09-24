package indexing_engine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class VectorGenerator {
    private static final String JobName = "indexer";
    public static final String OutputDir = "document_vectors";

    static final String StringIDF = "indexer.idf";
    static final String StringWords = "indexer.words";

    public static Path run(Path path_docId2text, Path path_word2Id, Path path_word2IDF, Path path_outDir)
            throws Exception {
        Configuration conf = new Configuration();
        // Add words and idf to conf
        conf.set(StringWords, path_word2Id.toString());
        conf.set(StringIDF, path_word2IDF.toString());

        Job job = Job.getInstance(conf, JobName);
        job.setJarByClass(VectorGenerator.class);
        job.setMapperClass(VectorGenMapper.class);
        job.setReducerClass(VectorGenReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, path_docId2text);
        Path out = new Path(path_outDir, OutputDir);
        FileOutputFormat.setOutputPath(job, out);

        if (job.waitForCompletion(true)) {
            return out;
        } else {
            throw new Exception("Indexer.run() was not completed");
        }
    }
}
