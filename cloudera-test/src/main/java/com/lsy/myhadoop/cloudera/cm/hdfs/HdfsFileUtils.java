//package com.lsy.myhadoop.cloudera.cm.hdfs;
//
//import com.google.common.io.ByteStreams;
//import org.apache.commons.lang3.tuple.Pair;
//import org.apache.hadoop.fs.*;
//
//import javax.annotation.Nonnull;
//import java.io.*;
//import java.nio.charset.Charset;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//import java.util.zip.ZipEntry;
//import java.util.zip.ZipInputStream;
//
///**
// * Created by lisiyu on 2020/4/23.
// */
//public class HdfsFileUtils {
//
//    private FileSystem fs ;
//
//    private HadoopClientService client;
//
//    public FileSystem getFileSytem() {
//
//        if(fs==null) {
//            synchronized (client) {
//                fs = client.getFileSystem();
//            }
//        }
//        return fs;
//    }
//
//    public Pair<Boolean,String> createFile(String filePath, InputStream data, boolean overwrite) {
//        Path p = new Path(filePath);
//        return createFile(p.getParent().toString(), p.getName(), data, overwrite);
//    }
//
//    public Pair<Boolean, String> createZipFile(String parentPath, String fileName,
//                                               InputStream data, boolean overwrite, boolean upzip) {
//        //如果无需解压 就按照普通文件方式上传到HDFS上
//        if(!upzip) {
//            return createFile(parentPath,fileName, data, overwrite);
//        }else {
//            ZipEntry entry = null;
//            FileSystem fs = getFileSytem();
//            int i=0;
//            try(ZipInputStream zip = new ZipInputStream(data)) {
//
//                while((entry=zip.getNextEntry())!=null) {
//                    i++;
//                    if(entry.getName().contains("/.")) {continue;}
//                    if(entry.isDirectory()) {
//                        fs.mkdirs(new Path(parentPath,entry.getName()));
//                    }else {
//                        if(i==1) {
//                            logger.info("name={}", entry.getName());
//                            data.close();
//                            //默认需要zip内部有一级目录 这样第一个读取的entry 就应该是个目录
//                            throw new RuntimeException("zip should contain fold first");
//                        }
//
//                        try(FSDataOutputStream fos =  fs.create(new Path(parentPath,entry.getName()),overwrite)){
//                            IOUtils.copy(zip,fos);
//                        }
//
//                    }
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//                return Pair.of(false, "unzip file fail:"+e.getMessage()+" -->"+entry);
//            }
//            if(i==0) {
//                return Pair.of(true, "empty or not zip file");
//            }else return Pair.of(true, "success upload zip file and upzip to "+i+" folds and files");
//        }
//
//    }
//
//    public Pair<Boolean, String> createFile(String parentPath, String fileName, InputStream data, boolean overwrite) {
//        Path pp = new Path(parentPath);
//        Path p = new Path(pp,fileName);
//        FSDataOutputStream os =  null;
//        try {
//            if(getFileSytem().exists(p)&&!overwrite) {
//                return Pair.of(false, "file exist,and cannot overwrite");
//            }
//
//            if(!getFileSytem().exists(pp)) {
//
//                boolean  r = getFileSytem().mkdirs(pp);
//                if(!r) {
//                    return  Pair.of(false, "make dir "+pp.toString()+" fail");
//                }else {
//                    logStr(true, "create fold", p, "");
//                }
//            }
//            os = getFileSytem().create(p, overwrite);
//            long copySize = ByteStreams.copy(data, os);
//            logStr(true, "create ", p, "");
//            return Pair.of(true, "success create file:" + copySize);
//        } catch (IOException e) {
//            logStr(false, "createFile", p, e.getMessage());
//            return Pair.of(false, e.getMessage());
//        }finally {
//            IOUtils.close(data);
//            IOUtils.close(os);
//        }
//    }
//
//    public Pair<Boolean, String> createFile(String filePath, String data, boolean overwrite) {
//        return createFile(filePath, new ByteArrayInputStream(data.getBytes()), overwrite);
//    }
//
//
//    public Pair<Boolean,String> createDir(String parentPath, String dirPath) {
//        Path pp = new Path(parentPath);
//        Path realPath = new Path(pp, dirPath);
//        try {
//            if(!getFileSytem().exists(pp)) {
//                getFileSytem().mkdirs(pp);
//            }
//            getFileSytem().mkdirs(realPath);
//            logStr(false, "delete", realPath, "");
//            return Pair.of(true, "create success");
//        } catch (IOException e) {
//            logStr(false, "create", realPath, e.getMessage());
//            return Pair.of(false, "create fail:"+e.getMessage());
//        }
//    }
//
//    public List<FileInfo> listTree(String filePath, boolean recursive) {
//        try {
//            RemoteIterator<LocatedFileStatus> fss = getFileSytem().listFiles(new Path(filePath), recursive);
//            List<FileInfo> result = new LinkedList<>();
//            while(fss.hasNext()) {
//                result.add(HadoopMetaInfoConverterUtils.convertFileInfo(fss.next()));
//            }
//            return result;
//        } catch (IllegalArgumentException | IOException e) {
//            logger.error(e.getMessage());
//            throw new RuntimeException("read file list error:"+e.getMessage());
//        }
//    }
//
//    public List<FileInfo> list(String filePath) {
//        try {
//            return Stream.of(getFileSytem().listStatus(new Path(filePath)))
//                    .map(HadoopMetaInfoConverterUtils::convertFileInfo).collect(Collectors.toList());
//
//        } catch (IllegalArgumentException | IOException e) {
//            logger.error(e.getMessage());
//            throw new RuntimeException("read file list error:"+e.getMessage());
//        }
//    }
//
//    public FileInfo get(String filePath) {
//        try {
//
//            FileStatus fsx = getFileSytem().getFileStatus(new Path(filePath));
//            return HadoopMetaInfoConverterUtils.convertFileInfo(fsx);
//        } catch (IllegalArgumentException | IOException e) {
//            logger.error(e.getMessage());
//            throw new RuntimeException("read file info error:"+e.getMessage());
//        }
//    }
//
//    public Pair<Boolean,String> createDir(@Nonnull String dirPath) {
//        Path p = new Path(dirPath);
//        try {
//            boolean success =  getFileSytem().mkdirs(p);
//            logStr(success, "createDir", p, "");
//            return Pair.of(success, "create dir:"+success);
//        } catch (IllegalArgumentException | IOException e) {
//            logStr(false, "createDir", p, e.getMessage());
//            return Pair.of(false, "create dir fail:"+e.getMessage());
//        }
//    }
//
//    public Pair<Boolean, String> delete(String filePath, boolean keepParentFolder, boolean recursion) {
//        try {
//            Path path = new Path(filePath);
//            FileStatus fileStatus = getFileSytem().getFileStatus(path);
//
//            if (fileStatus.isDirectory() && keepParentFolder) {
//                FileStatus[] listStatus = fs.listStatus(path);
//
//                for (FileStatus fstatus : listStatus) {
//                    getFileSytem().delete(fstatus.getPath(), recursion);
//                }
//            } else {
//                getFileSytem().delete(path, recursion);
//            }
//
//            return Pair.of(true, "delete :success");
//        } catch (Exception e) {
//            logger.error("删除文件异常", e);
//            return Pair.of(false, "delete :"+e.getMessage());
//        }
//    }
//
//    public Pair<Boolean, String> delete(String parentPath, String fileName, boolean recursion) {
//        Path p = new Path(parentPath,fileName);
//        try {
//            boolean success =  getFileSytem().delete(p, recursion);
//
//            logStr(success, "delete", p, "");
//
//            return Pair.of(success, "delete :"+success);
//        } catch (IllegalArgumentException | IOException e) {
//            logger.error("删除异常", e);
//            logStr(false, "delete", p, e.getMessage());
//            return Pair.of(false, "delete :"+e.getMessage());
//        }
//    }
//
//    public InputStream read(String filePath) {
//        Path p = new Path(filePath);
//        try {
//            return getFileSytem().open(p);
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new RuntimeException("read file fail:"+filePath);
//        }
//    }
//
//    public String readLines(String filePath) {
//        try (InputStream is =  read(filePath)){
//            BufferedReader br = new BufferedReader(new InputStreamReader(is, Charset.defaultCharset()));
//            return br.lines().collect(Collectors.joining("\n"));
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new RuntimeException(e.getMessage());
//        }
//
//    }
//
//    public List<String> read(String parentPath, String fileName) {
//        return FileTools.readLines(getFileSytem(), parentPath + fileName);
//    }
//
//    public Pair<Boolean,String> createUserDir(String username) {
//        Path path = new Path("/user",username);
////		System.out.println("------------");
////		System.out.println(path.toString());
//        System.setProperty("HADOOP_USER_NAME", "hdfs");
//        try {
//            boolean exist = getFileSytem().exists(path);
//            if(exist)return Pair.of(true, "exist");
//
//            boolean create = getFileSytem().mkdirs(path);
//            if(!create)return Pair.of(false, "create fail");
//            //修改下权限
//            getFileSytem().setOwner(path, username, null);
//
//            return Pair.of(true, "create success");
//        } catch (Exception e) {
//            e.printStackTrace();
//            throw new RuntimeException("create user dir fail:"+username+",exception:"+e.getMessage());
//        }
//    }
//
//
//    public Pair<Boolean,String> checkFileSystemAvaliable() {
//
//        return Pair.of(true, "");
//    }
//
//    public static void logStr(boolean success,String method,Path path,String msg) {
//        if(success) {
//            logger.info("{} {}:{}, msg: {}","success",method,path.toUri(),msg);
//        }else {
//            logger.error("{} {}:{}, msg: {}","fail",method,path.toUri(),msg);
//        }
//    }
//}
