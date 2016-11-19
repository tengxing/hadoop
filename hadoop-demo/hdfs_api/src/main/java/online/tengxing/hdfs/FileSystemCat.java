package online.tengxing.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * 使用FileSystem API 读取数据
 * Created by tengxing on 16-11-17.
 * mail tengxing7452@163.com
 */
public class FileSystemCat {

    public static void readHdfs(String url) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(url), conf);
        InputStream in = null;
        try {
            in = fs.open(new Path(url));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }

    public static void main(String[] args) throws IOException {

        readHdfs("hdfs://localhost:9000/user/tengxing/input/dd");
    }
}

