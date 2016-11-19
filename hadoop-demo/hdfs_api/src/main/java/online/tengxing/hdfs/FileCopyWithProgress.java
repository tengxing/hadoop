package online.tengxing.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * 创建目录放目录
 * Created by tengxing on 16-11-17.
 mail tengxing7452@163.com
 */
public class FileCopyWithProgress {
    public static void fileCopy(String localFile, String hdfsFile) throws IOException{
        InputStream in = new BufferedInputStream(new FileInputStream(localFile));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsFile),conf);
        OutputStream out  = fs.create(new Path(hdfsFile),new Progressable(){
            public void progress(){
                System.out.println("*"); //上传进度 64k一个
            }
        });

        IOUtils.copyBytes(in, out, 4096,true);
    }

    public static void main(String[] args) throws IOException {
        fileCopy("/home/tengxing/tmp/aa", "hdfs://localhost:9000/user/tengxing/output/");
        /*
        * 说明Hadoop的NameNode处在安全模式下，那什么是Hadoop的安全模式呢？
            在分布式文件系统启动的时候，开始的时候会有安全模式，当分布式文件系统处于安全模式的情况下，文件系统中的内容不允许修改也不允许删除，直到安全模式结束。安全模式主要是为了系统启动的时候检查各个DataNode上数据块的有效性，同时根据策略必要的复制或者删除部分数据块。运行期通过命令也可以进入安全模式。在实践过程中，系统启动的时候去修改和删除文件也会有安全模式不允许修改的出错提示，只需要等待一会儿即可。
            现在就清楚了，那现在要解决这个问题，我想让Hadoop不处在safe mode 模式下，能不能不用等，直接解决呢？
            答案是可以的，只要在Hadoop的目录下输入：
            bin/hadoop dfsadmin -safemode leave
            也就是关闭Hadoop的安全模式，这样问题就解决了
        * */
    }
}
