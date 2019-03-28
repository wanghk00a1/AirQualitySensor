package hk.hku.spark.utils

import java.io.ByteArrayInputStream
import java.net.URI

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.zookeeper.common.IOUtils

/**
  * 通过scala操作HDFS
  */
object HDFSUtils {
  val hdfsUrl = "hdfs://gpu7:9000"
  var realUrl = ""

  /**
    * make a new dir in the hdfs
    *
    * @param dir the dir may like '/tmp/testdir'
    * @return boolean true-success, false-failed
    */
  def mkdir(dir: String): Boolean = {
    var result = false
    if (StringUtils.isNoneBlank(dir)) {
      realUrl = hdfsUrl + dir
      val config = new Configuration()
      val fs = FileSystem.get(URI.create(realUrl), config)
      if (!fs.exists(new Path(realUrl))) {
        fs.mkdirs(new Path(realUrl))
      }
      fs.close()
      result = true
    }
    result
  }

  /**
    * delete a dir in the hdfs.
    * if dir not exists, it will throw FileNotFoundException
    *
    * @param dir the dir may like '/tmp/testdir'
    * @return boolean true-success, false-failed
    */
  def deleteDir(dir: String): Boolean = {
    var result = false
    if (StringUtils.isNoneBlank(dir)) {
      realUrl = hdfsUrl + dir
      val config = new Configuration()
      val fs = FileSystem.get(URI.create(realUrl), config)
      fs.delete(new Path(realUrl), true)
      fs.close()
      result = true
    }
    result
  }

  /**
    * create a new file in the hdfs. notice that the toCreateFilePath is the full path
    * and write the content to the hdfs file.
    *
    * create a new file in the hdfs.
    * if dir not exists, it will create one
    *
    * @param newFile new file path, a full path name, may like '/tmp/test.txt'
    * @param content file content
    * @return boolean true-success, false-failed
    **/
  def createNewHDFSFile(newFile: String, content: String): Boolean = {
    var result = false
    if (StringUtils.isNoneBlank(newFile) && null != content) {
      realUrl = hdfsUrl + newFile
      val config = new Configuration()
      val hdfs = FileSystem.get(URI.create(realUrl), config)
      val os = hdfs.create(new Path(realUrl))
      os.write(content.getBytes("UTF-8"))
      os.close()
      hdfs.close()
      result = true
    }
    result
  }

  /**
    * delete the hdfs file
    *
    * @param hdfsFile a full path name, may like '/tmp/test.txt'
    * @return boolean true-success, false-failed
    */
  def deleteHDFSFile(hdfsFile: String): Boolean = {
    var result = false
    if (StringUtils.isNoneBlank(hdfsFile)) {
      realUrl = hdfsUrl + hdfsFile
      val config = new Configuration()
      val hdfs = FileSystem.get(URI.create(realUrl), config)
      val path = new Path(realUrl)
      val isDeleted = hdfs.delete(path, true)
      hdfs.close()
      result = isDeleted
    }
    result
  }

  /**
    * read the hdfs file content
    *
    * @param hdfsFile a full path name, may like '/tmp/test.txt'
    * @return byte[] file content
    */
  def readHDFSFile(hdfsFile: String): Array[Byte] = {
    var result = new Array[Byte](0)
    if (StringUtils.isNoneBlank(hdfsFile)) {
      realUrl = hdfsUrl + hdfsFile
      val config = new Configuration()
      val hdfs = FileSystem.get(URI.create(realUrl), config)
      val path = new Path(realUrl)
      if (hdfs.exists(path)) {
        val inputStream = hdfs.open(path)
        val stat = hdfs.getFileStatus(path)
        val length = stat.getLen.toInt
        val buffer = new Array[Byte](length)
        inputStream.readFully(buffer)
        inputStream.close()
        hdfs.close()
        result = buffer
      }
    }
    result
  }

  /**
    * append something to file dst
    *
    * @param hdfsFile a full path name, may like '/tmp/test.txt'
    * @param content  string
    * @return boolean true-success, false-failed
    */
  def append(hdfsFile: String, content: String): Boolean = {
    var result = false
    if (StringUtils.isNoneBlank(hdfsFile) && null != content) {
      realUrl = hdfsUrl + hdfsFile
      val config = new Configuration()
      config.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
      config.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
      val hdfs = FileSystem.get(URI.create(realUrl), config)
      val path = new Path(realUrl)
      if (hdfs.exists(path)) {
        val inputStream = new ByteArrayInputStream(content.getBytes())
        val outputStream = hdfs.append(path)
        IOUtils.copyBytes(inputStream, outputStream, 4096, true)
        outputStream.close()
        inputStream.close()
        hdfs.close()
        result = true
      }
    } else {
      HDFSUtils.createNewHDFSFile(hdfsFile, content);
      result = true
    }
    result
  }
}
