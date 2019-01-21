package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.FileInputStream;
import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HdfsApplicationTests {

  @Test
  public void contextLoads() {
  }

  /**
   * 文件下载
   *
   * @throws IOException
   */
  @Test
  public void test1() throws IOException {
    Configuration configuration = new Configuration();
    //添加地址属性
    configuration.set("fs.default.name", "hdfs://hadoop:8020");
    FileSystem fileSystem = FileSystem.get(configuration);
    //Hadoop上的文件路径
    FSDataInputStream inputStream = fileSystem.open(new Path("/Idea/test"));
    IOUtils.copyBytes(inputStream, System.out, 1024, true);
  }

  /**
   * 文件上传
   *
   * @throws IOException
   */
  @Test
  public void test2() throws IOException {
    Configuration configuration = new Configuration();
    configuration.set("fs.default.name", "hdfs://hadoop:8020");
    FileSystem fileSystem = FileSystem.get(configuration);
    //在Hadoop上创建路径名及文件
    FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/Idea/test"));
    //Hadoop文件的内容引用
    FileInputStream inputStream = new FileInputStream("e://q.txt");
    IOUtils.copyBytes(inputStream, fsDataOutputStream, 1024, true);
  }

}

