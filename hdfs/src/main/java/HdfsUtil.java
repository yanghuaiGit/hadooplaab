import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileOutputStream;
import java.net.URI;

public class HdfsUtil {

    public static void main(String[] args) throws Exception {

        upload();
        downLoad();
    }

    public static void upload() throws Exception {
        Configuration configuration = new Configuration();
//        configuration.set("fs.hds.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.set("fs.defaultFS", "hdfs://v1:8020");

        URI uri = new URI("hdfs://v1:8020");
//        FileSystem fileSystem = FileSystem.newInstance(configuration);
//        fileSystem.copyFromLocalFile(new Path("/Users/yanghuai/Desktop/a.json"), new Path("/test2/test2.json"));
        FileSystem root = FileSystem.get(uri, configuration,"hdfs");
        root.copyFromLocalFile(new Path("/Users/yanghuai/Desktop/test.txt"), new Path("/test.txt"));
    }

    public static void downLoad() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://v1:8020/");
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataInputStream open = fileSystem.open(new Path("hdfs://v1:8020/test.txt"));
        FileOutputStream fileOutputStream = new FileOutputStream("/Users/yanghuai/Desktop/test1.txt");
        IOUtils.copy(open, fileOutputStream);
    }
}
