package online.tengxing.hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.StringTokenizer;

/**
 *使用Hadoop URL读取数据
 *
 * Created by tengxing on 16-11-15.
 * mail tengxing7452@163.com
 */
public class URLCat {
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void readHdfs(String url) throws Exception {
        InputStream in = null;
        try {
            in = new URL(url).openStream();
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }

    public static void main(String[] args) throws Exception {
        readHdfs("hdfs://localhost:9000/user/tengxing/input/dd");
    }
}

