package edu.sjtu;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Options.ChecksumOpt;

import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;

import org.apache.hadoop.util.DataChecksum;
import org.junit.Test;


public class TestSleeve {
  private static final Log LOG = LogFactory.getLog(TestSleeve.class);

  private Configuration conf = new HdfsConfiguration();

  @Test(timeout = 60000)
  public void testMkdir() throws IOException {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    DistributedFileSystem dfs = (DistributedFileSystem) cluster.getFileSystem();

    boolean flag = false;
    try {
      // Create a dir in root dir, should succeed
      assertTrue(dfs.mkdir(new Path("/mkdir-a"),
          FsPermission.getDefault()));
      assertTrue(dfs.exists(new Path("/mkdir-a")));

      LOG.info("The Path is:" + new Path("/mkdir-a").toString());
      
      FSNamesystem ns = cluster.getNamesystem();
      ns.renameTo(new Path("/mkdir-a").toString(), new Path("/mkdir-b").toString());

      LOG.info("Rename finished...");

      try {
        dfs.mkdir(new Path("/mkdir-a"), FsPermission.getDefault());
      } catch (IOException ioe) {
        HdfsFileStatus fileStatus = ns.getFileInfo(new Path("/mkdir-a").toString(), false);
        LOG.info("Entering IOException section...");
        if (fileStatus != null && fileStatus.isDir())
          flag = true;
      }
    } finally {
      dfs.close();
      cluster.shutdown();
    }

    assertTrue(flag);
  }

  @Test(timeout = 60000)
  public void testCreate() throws Exception {
    MiniDFSCluster cluster = null;
    Path testBasePath = new Path("/test");
    // create args 
    Path path1 = new Path(testBasePath, "file1");
    Path path2 = new Path(testBasePath, "file2");
    ChecksumOpt opt = new ChecksumOpt(DataChecksum.Type.CRC32C, 512);

    // common args
    FsPermission perm = FsPermission.getDefault().applyUMask(
        FsPermission.getUMask(conf));
    EnumSet<CreateFlag> flags = EnumSet.of(CreateFlag.OVERWRITE,
        CreateFlag.CREATE);
    short repl = 1;
    boolean flag = false;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      FileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(testBasePath);

      // create two files with different checksum types
      FSDataOutputStream out1 = dfs.create(path1, perm, flags, 4096, repl,
          131072L, null, opt);
      FSDataOutputStream out2 = dfs.create(path2, perm, flags, 4096, repl,
          131072L, null, opt);

      for (int i = 0; i < 1024; i++) {
        out1.write(i);
        out2.write(i);
      }
      out1.close();
      out2.close();

      // rename this file.
      FSNamesystem ns = cluster.getNamesystem();
      ns.renameTo(new Path("/test/file1").toString(), new Path("/test/file3").toString());

      try {
        FSDataOutputStream out3 = dfs.create(path1, perm, flags, 4096, repl,
          131072L, null, opt);
      } catch (IOException ioe) {
        HdfsFileStatus fileStatus = ns.getFileInfo(new Path("/test/file1").toString(), false);
        LOG.info("Entering IOException section...");
        if (fileStatus != null && !fileStatus.isDir())
          flag = true;
      }
    } finally {
      if (cluster != null) {
        cluster.getFileSystem().delete(testBasePath, true);
        cluster.shutdown();
      }
    }

    assertTrue(flag);
  }
}