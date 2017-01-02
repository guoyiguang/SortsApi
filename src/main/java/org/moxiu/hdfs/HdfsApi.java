package org.moxiu.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * Created by Administrator on 2017/1/2.
 * 此类用于对HDFS进行操作。是最基本的一些API。
 */
public class HdfsApi {
    public static void main(String args[])
    {

    }
    /**
     * 使用io流的方式将本地文件上传到hdfs
     */
    public void ioUpload() throws Exception
    {
        //1.创建配置文件对象
        Configuration conf = new Configuration();
        //2.给配置文件对象配置参数，也可以把配置文件Core-site.xml拷贝到工程下面。
        conf.set("fs.defaultFS","hdfs://master:9000");
        //3.创建抽象类FileSystem并将配置文件传进去，底层通过RPC框架获取Distrubutedsystem分布式系统的类。
        FileSystem fs  = FileSystem.get(conf);
        //4.定义路径 需要上传到hdfs的路径。即目标路径
        Path p = new Path("hdfs://master:9000/demo.txt");
        //5.获取一个流，在HDFS中存数据，使用的是created 在已存在的文件添加数据使用append
        FSDataOutputStream fsd = fs.append(p);
        //6.getpos获取文件的初始位置
        long s = fsd.getPos();
        System.out.print(s);
        //7.获取本地读取流
        FileInputStream fin = new FileInputStream("/home/xxx/demo.txt");
        IOUtils.copy(fin,fsd);
    }
    /**
     * 使用io流从HDFS上下载文件到本地。
     */
    public void ioDowlond() throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://master:9000");
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path("hdfs://master:9000/demo.txt");
        //创建HDFS的读取流
        FSDataInputStream fis = fs.open(p);
        //定义本地输出流，写入的路径
        FileOutputStream fos = new FileOutputStream("/home/xxx/demo2.txt");
        byte [] by = new byte[1024];
        fis.read(by);
        fos.write(by);
        fis.close();
        fos.close();
    }
    /**
     * 查看hdfs上文件的元数据信息。
     */
    public void getStatus() throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://master:9000");
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path("hdfs://master:9000/demo.txt");
        //获取文件元数据对象。
        FileStatus status = fs.getFileStatus(p);
        //查看块大小
        System.out.println(status.getBlockSize());
        //查看文件的路径
        System.out.println(status.getPath());
        //查看文件的长度
        System.out.println(status.getLen());
        //查看文件的最后一次修改的时间
        System.out.println(status.getModificationTime());
        //查看文件的权限
        System.out.println(status.getPermission());
        //查看文件的备份数
        System.out.println(status.getReplication());
        //查看文件的所属用户 所属组
        System.out.println(status.getOwner()+"   "+status.getGroup());
        //查看符号链接
        System.out.print(status.getSymlink());

    }
    /**
     * 使用包装好的类进行上传文件
     */
    public void upload() throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://master:9000");
        FileSystem fs = FileSystem.get(conf);
        // 使用已经包装好的类copytolocalfile将文件从hdfs拷贝到本地
        fs.copyToLocalFile(new Path("hdfs://master:9000/demo.txt"),new Path("/home/xxx/demo.txt"));
    }
    /**
     * 使用包装好的类在hdfs上创建目录
     */
    public void mkdir() throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://master:9000");
        //创建顶层文件系统类
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(new Path("/a/b"));
    }
    /**
     * 使用包装好的类在hdfs上删除目录
     */
    public void delete() throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://master:9000");
        //创建顶层文件系统类
        FileSystem fs = FileSystem.get(conf);
        //true  支持递归删除
        fs.delete(new Path("/a/b"),true);
    }
    /**
     * 显示目录中的文件
     */
    public void selectDir() throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://Master:9000");
        FileSystem fs = FileSystem.get(conf);
            // 获取元数据迭代器
            RemoteIterator<LocatedFileStatus> Files = fs.listFiles(new Path("/flume/"),true);
            // 获取元数据对象
            while(Files.hasNext())
            {
                LocatedFileStatus status = Files.next();
                // 这个方法能够迭代获取一个目录下的所有文件。
                //获取文件的路径以及名称
                String name = status.getPath().getName();
            }
        System.out.print("+++++++++++++++++++++++++++++++++++++++++++++++");
        //以下可以不可以迭代输出
        FileStatus[] status = fs.listStatus(new Path("/"));
        for(FileStatus s :status)
        {
            System.out.print(s.getPath().getName());
            System.out.println(s.getPath().getName()+"是否是文件"+s.isDirectory());
        }
    }
}
