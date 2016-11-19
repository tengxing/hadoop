package online.tengxing.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * 显示文件目录以及结构
 * Created by tengxing on 16-11-17.
 * mail tengxing7452@163.com
 */
public class ListStatus {
    public static void readStatus(String url) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(url), conf);
        Path[] paths = new Path[1];
        paths[0] = new Path(url);
        FileStatus[] status = fs.listStatus(paths);
        Path[] listedPaths = FileUtil.stat2Paths(status);
        for (Path p : listedPaths) {
            System.out.println(p);
        }
    }

    public static void main(String[] args) throws IOException {
        readStatus("hdfs://localhost:9000/user/tengxing/output/");
    }
}
