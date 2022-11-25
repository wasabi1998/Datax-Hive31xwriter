package com.alibaba.datax.plugin.writer.hive31xwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.util.ColumnTypeUtil;
import com.alibaba.datax.plugin.unstructuredstorage.util.HdfsUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public  class HiveHelper {
    public static final Logger LOG = LoggerFactory.getLogger(HiveWriter.Job.class);

    public FileSystem fileSystem = null;
    public JobConf conf = null;
    public org.apache.hadoop.conf.Configuration hadoopConf = null;
    public static final String HADOOP_SECURITY_AUTHENTICATION_KEY = "hadoop.security.authentication";
    public static final String HDFS_DEFAULTFS_KEY = "fs.defaultFS";

    // Kerberos
    private Boolean haveKerberos = false;
    private String  kerberosKeytabFilePath;
    private String  kerberosPrincipal;

    //recursionPathsArr
    private static ArrayList<String> recursionPathsArr = new ArrayList<>();

    public void getFileSystem(String defaultFS, Configuration taskConfig){
        hadoopConf = new org.apache.hadoop.conf.Configuration();

        Configuration hadoopSiteParams = taskConfig.getConfiguration(Key.HADOOP_CONFIG);
        JSONObject hadoopSiteParamsAsJsonObject = JSON.parseObject(taskConfig.getString(Key.HADOOP_CONFIG));
        if (null != hadoopSiteParams) {
            Set<String> paramKeys = hadoopSiteParams.getKeys();
            for (String each : paramKeys) {
                hadoopConf.set(each, hadoopSiteParamsAsJsonObject.getString(each));
            }
        }
        hadoopConf.set(HDFS_DEFAULTFS_KEY, defaultFS);

        //是否有Kerberos认证
        this.haveKerberos = taskConfig.getBool(Key.HAVE_KERBEROS, false);
        if(haveKerberos){
            this.kerberosKeytabFilePath = taskConfig.getString(Key.KERBEROS_KEYTAB_FILE_PATH);
            this.kerberosPrincipal = taskConfig.getString(Key.KERBEROS_PRINCIPAL);
            hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION_KEY, "kerberos");
        }
        this.kerberosAuthentication(this.kerberosPrincipal, this.kerberosKeytabFilePath);
        conf = new JobConf(hadoopConf);
        try {
            if (StringUtils.isNotBlank(taskConfig.getString(Key.SUPERUSER)) && !haveKerberos) {
                System.setProperty("HADOOP_USER_NAME", taskConfig.getString(Key.SUPERUSER));
                fileSystem = FileSystem.get(new URI(hadoopConf.get(HDFS_DEFAULTFS_KEY)), conf, taskConfig.getString(Key.SUPERUSER));
            }else {
                fileSystem = FileSystem.get(conf);
            }
        } catch (IOException e) {
            String message = String.format("获取FileSystem时发生网络IO异常,请检查您的网络是否正常!HDFS地址：[%s]",
                    "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }catch (Exception e) {
            String message = String.format("获取FileSystem失败,请检查HDFS地址是否正确: [%s]",
                    "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }

        if(null == fileSystem || null == conf){
            String message = String.format("获取FileSystem失败,请检查HDFS地址是否正确: [%s]",
                    "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, message);
        }
    }

    private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath){
        if(haveKerberos && StringUtils.isNotBlank(this.kerberosPrincipal) && StringUtils.isNotBlank(this.kerberosKeytabFilePath)){
            UserGroupInformation.setConfiguration(this.hadoopConf);
            try {
                UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
            } catch (Exception e) {
                String message = String.format("kerberos认证失败,请确定kerberosKeytabFilePath[%s]和kerberosPrincipal[%s]填写正确",
                        kerberosKeytabFilePath, kerberosPrincipal);
                LOG.error(message);
                throw DataXException.asDataXException(HiveWriterErrorCode.KERBEROS_LOGIN_ERROR, e);
            }
        }
    }

    /**
     *获取指定目录下的文件列表
     * @param dir
     * @return
     * 拿到的是文件全路径，
     * eg：hdfs://10.101.204.12:9000/user/hive/warehouse/writer.db/text/test.textfile
     */
    public String[] hdfsDirList(String dir){
        Path path = new Path(dir);
        String[] files = null;
        try {
            FileStatus[] status = fileSystem.listStatus(path);
            files = new String[status.length];
            for(int i=0;i<status.length;i++){
                files[i] = status[i].getPath().toString();
            }
        } catch (IOException e) {
            String message = String.format("获取目录[%s]文件列表时发生网络IO异常,请检查您的网络是否正常！", dir);
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return files;
    }

    /**
     * 获取以fileName__ 开头的文件列表
     * @param dir
     * @param fileName
     * @return
     */
    public Path[] hdfsDirList(String dir,String fileName){
        Path path = new Path(dir);
        Path[] files = null;
        String filterFileName = fileName + "__*";
        try {
            PathFilter pathFilter = new GlobFilter(filterFileName);
            FileStatus[] status = fileSystem.listStatus(path,pathFilter);
            files = new Path[status.length];
            for(int i=0;i<status.length;i++){
                files[i] = status[i].getPath();
            }
        } catch (IOException e) {
            String message = String.format("获取目录[%s]下文件名以[%s]开头的文件列表时发生网络IO异常,请检查您的网络是否正常！",
                    dir,fileName);
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return files;
    }

    public boolean isPathexists(String filePath) {
        Path path = new Path(filePath);
        boolean exist = false;
        try {
            exist = fileSystem.exists(path);
        } catch (IOException e) {
            String message = String.format("判断文件路径[%s]是否存在时发生网络IO异常,请检查您的网络是否正常！",
                    "message:filePath =" + filePath);
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return exist;
    }

    public boolean isPathDir(String filePath) {
        Path path = new Path(filePath);
        boolean isDir = false;
        try {
            isDir = fileSystem.isDirectory(path);
        } catch (IOException e) {
            String message = String.format("判断路径[%s]是否是目录时发生网络IO异常,请检查您的网络是否正常！", filePath);
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return isDir;
    }

    public void deleteFiles(Path[] paths){
        for(int i=0;i<paths.length;i++){
            LOG.info(String.format("delete file [%s].", paths[i].toString()));
            try {
                fileSystem.delete(paths[i],true);
            } catch (IOException e) {
                String message = String.format("删除文件[%s]时发生IO异常,请检查您的网络是否正常！",
                        paths[i].toString());
                LOG.error(message);
                throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
            }
        }
    }

    public void deleteDir(Path path){
        LOG.info(String.format("start delete tmp dir [%s] .",path.toString()));
        try {
            if(isPathexists(path.toString())) {
                fileSystem.delete(path, true);
            }
        } catch (Exception e) {
            String message = String.format("删除临时目录[%s]时发生IO异常,请检查您的网络是否正常！", path.toString());
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        LOG.info(String.format("finish delete tmp dir [%s] .",path.toString()));
    }

    public void renameFile(HashSet<String> tmpFiles, HashSet<String> endFiles){
        Path tmpFilesParent = null;
        if(tmpFiles.size() != endFiles.size()){
            String message = String.format("临时目录下文件名个数与目标文件名个数不一致!");
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.HDFS_RENAME_FILE_ERROR, message);
        }else{
            try{
                for (Iterator it1=tmpFiles.iterator(),it2=endFiles.iterator();it1.hasNext()&&it2.hasNext();){
                    String srcFile = it1.next().toString();
                    String dstFile = it2.next().toString();
                    Path srcFilePah = new Path(srcFile);
                    Path dstFilePah = new Path(dstFile);
                    if(tmpFilesParent == null){
                        tmpFilesParent = srcFilePah.getParent();
                    }
                    LOG.info(String.format("start rename file [%s] to file [%s].", srcFile,dstFile));
                    boolean renameTag = false;
                    long fileLen = fileSystem.getFileStatus(srcFilePah).getLen();
                    if(fileLen>0){
                        renameTag = fileSystem.rename(srcFilePah,dstFilePah);
                        if(!renameTag){
                            String message = String.format("重命名文件[%s]失败,请检查您的网络是否正常！", srcFile);
                            LOG.error(message);
                            throw DataXException.asDataXException(HiveWriterErrorCode.HDFS_RENAME_FILE_ERROR, message);
                        }
                        LOG.info(String.format("finish rename file [%s] to file [%s].", srcFile,dstFile));
                    }else{
                        LOG.info(String.format("文件［%s］内容为空,请检查写入是否正常！", srcFile));
                    }
                }
            }catch (Exception e) {
                String message = String.format("重命名文件时发生异常,请检查您的网络是否正常！");
                LOG.error(message);
                throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
            }finally {
                deleteDir(tmpFilesParent);
            }
        }
    }

    //关闭FileSystem
    public void closeFileSystem(){
        try {
            fileSystem.close();
        } catch (IOException e) {
            String message = String.format("关闭FileSystem时发生IO异常,请检查您的网络是否正常！");
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
    }

    //textfile格式文件
    public  FSDataOutputStream getOutputStream(String path){
        Path storePath = new Path(path);
        FSDataOutputStream fSDataOutputStream = null;
        try {
            fSDataOutputStream = fileSystem.create(storePath);
        } catch (IOException e) {
            String message = String.format("Create an FSDataOutputStream at the indicated Path[%s] failed: [%s]",
                    "message:path =" + path);
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
        return fSDataOutputStream;
    }

    /**
     * 写textfile类型文件
     * @param lineReceiver
     * @param config
     * @param fileName
     * @param taskPluginCollector
     */
    public void textFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
                                   TaskPluginCollector taskPluginCollector){
        char fieldDelimiter = config.getChar(Key.FIELD_DELIMITER);
        List<Configuration>  columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS,null);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
        String attempt = "attempt_"+dateFormat.format(new Date())+"_0001_m_000000_0";
        Path outputPath = new Path(fileName);
        //todo 需要进一步确定TASK_ATTEMPT_ID
        conf.set(JobContext.TASK_ATTEMPT_ID, attempt);
        FileOutputFormat outFormat = new TextOutputFormat();
        outFormat.setOutputPath(conf, outputPath);
        outFormat.setWorkOutputPath(conf, outputPath);
        if(null != compress) {
            Class<? extends CompressionCodec> codecClass = getCompressCodec(compress);
            if (null != codecClass) {
                outFormat.setOutputCompressorClass(conf, codecClass);
            }
        }
        try {
            RecordWriter writer = outFormat.getRecordWriter(fileSystem, conf, outputPath.toString(), Reporter.NULL);
            Record record = null;
            while ((record = lineReceiver.getFromReader()) != null) {
                MutablePair<Text, Boolean> transportResult = transportOneRecord(record, fieldDelimiter, columns, taskPluginCollector);
                if (!transportResult.getRight()) {
                    writer.write(NullWritable.get(),transportResult.getLeft());
                }
            }
            writer.close(Reporter.NULL);
        } catch (Exception e) {
            String message = String.format("写文件文件[%s]时发生IO异常,请检查您的网络是否正常！", fileName);
            LOG.error(message);
            Path path = new Path(fileName);
            deleteDir(path.getParent());
            throw DataXException.asDataXException(HiveWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
    }

    public static MutablePair<Text, Boolean> transportOneRecord(
            Record record, char fieldDelimiter, List<Configuration> columnsConfiguration, TaskPluginCollector taskPluginCollector) {
        MutablePair<List<Object>, Boolean> transportResultList =  transportOneRecord(record,columnsConfiguration,taskPluginCollector);
        //保存<转换后的数据,是否是脏数据>
        MutablePair<Text, Boolean> transportResult = new MutablePair<Text, Boolean>();
        transportResult.setRight(false);
        if(null != transportResultList){
            Text recordResult = new Text(StringUtils.join(transportResultList.getLeft(), fieldDelimiter));
            transportResult.setRight(transportResultList.getRight());
            transportResult.setLeft(recordResult);
        }
        return transportResult;
    }

    public Class<? extends CompressionCodec>  getCompressCodec(String compress){
        Class<? extends CompressionCodec> codecClass = null;
        if(null == compress){
            codecClass = null;
        }else if("GZIP".equalsIgnoreCase(compress)){
            codecClass = org.apache.hadoop.io.compress.GzipCodec.class;
        }else if ("BZIP2".equalsIgnoreCase(compress)) {
            codecClass = org.apache.hadoop.io.compress.BZip2Codec.class;
        }else if("SNAPPY".equalsIgnoreCase(compress)){
            //todo 等需求明确后支持 需要用户安装SnappyCodec
            codecClass = org.apache.hadoop.io.compress.SnappyCodec.class;
            // org.apache.hadoop.hive.ql.io.orc.ZlibCodec.class  not public
            //codecClass = org.apache.hadoop.hive.ql.io.orc.ZlibCodec.class;
        }else {
            throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE,
                    String.format("目前不支持您配置的 compress 模式 : [%s]", compress));
        }
        return codecClass;
    }

    /**
     * 写orcfile类型文件
     * @param lineReceiver
     * @param config
     * @param fileName
     * @param taskPluginCollector
     */
    public void orcFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
                                  TaskPluginCollector taskPluginCollector){
        List<Configuration>  columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, null);
        List<String> columnNames = getColumnNames(columns);
        List<ObjectInspector> columnTypeInspectors = getColumnTypeInspectors(columns);
        StructObjectInspector inspector = (StructObjectInspector)ObjectInspectorFactory
                .getStandardStructObjectInspector(columnNames, columnTypeInspectors);

        OrcSerde orcSerde = new OrcSerde();

        FileOutputFormat outFormat = new OrcOutputFormat();
        if(!"NONE".equalsIgnoreCase(compress) && null != compress ) {
            Class<? extends CompressionCodec> codecClass = getCompressCodec(compress);
            if (null != codecClass) {
                outFormat.setOutputCompressorClass(conf, codecClass);
            }
        }
        try {
            RecordWriter writer = outFormat.getRecordWriter(fileSystem, conf, fileName, Reporter.NULL);
            Record record = null;
            while ((record = lineReceiver.getFromReader()) != null) {
                MutablePair<List<Object>, Boolean> transportResult =  transportOneRecord(record,columns,taskPluginCollector);
                if (!transportResult.getRight()) {
                    writer.write(NullWritable.get(), orcSerde.serialize(transportResult.getLeft(), inspector));
                }
            }
            writer.close(Reporter.NULL);
        } catch (Exception e) {
            String message = String.format("写文件文件[%s]时发生IO异常,请检查您的网络是否正常！", fileName);
            LOG.error(message);
            Path path = new Path(fileName);
            deleteDir(path.getParent());
            throw DataXException.asDataXException(HiveWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
    }

    /**
     * 写parquetfile类型文件
     * @param lineReceiver
     * @param config
     * @param fileName
     * @param taskPluginCollector
     */
    public void parquetFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
                                      TaskPluginCollector taskPluginCollector){
        List<Configuration>  columns = config.getListConfiguration(Key.COLUMN);
        String compress = config.getString(Key.COMPRESS, null);
        List<String> columnNames = getColumnNames(columns);
        List<ObjectInspector> columnTypeInspectors = getColumnTypeInspectors(columns);
        StructObjectInspector inspector = (StructObjectInspector)ObjectInspectorFactory
                .getStandardStructObjectInspector(columnNames, columnTypeInspectors);

        ParquetHiveSerDe parquetHiveSerDe = new ParquetHiveSerDe ();

        MapredParquetOutputFormat outFormat = new MapredParquetOutputFormat();
        if(!"NONE".equalsIgnoreCase(compress) && null != compress ) {
            Class<? extends CompressionCodec> codecClass = getCompressCodec(compress);
            if (null != codecClass) {
                outFormat.setOutputCompressorClass(conf, codecClass);
            }
        }
        try {
            Properties colProp= new Properties();
            colProp.setProperty("columns",String.join(",",columnNames));
            List<String> colTypes = new ArrayList<>();
            columns.forEach(col ->colTypes.add(col.getString(Key.TYPE)));
            colProp.setProperty("columns.types",String.join(",",colTypes));
            RecordWriter writer = (RecordWriter) outFormat.getHiveRecordWriter(conf,new Path(fileName), ObjectWritable.class,true,colProp,Reporter.NULL);
            Record record = null;
            while ((record = lineReceiver.getFromReader()) != null) {
                MutablePair<List<Object>, Boolean> transportResult =  transportOneRecord(record,columns,taskPluginCollector);
                if (!transportResult.getRight()) {
                    writer.write(null, parquetHiveSerDe.serialize(transportResult.getLeft(), inspector));
                }
            }
            writer.close(Reporter.NULL);
        } catch (Exception e) {
            String message = String.format("写文件文件[%s]时发生IO异常,请检查您的网络是否正常！", fileName);
            LOG.error(message);
            Path path = new Path(fileName);
            deleteDir(path.getParent());
            throw DataXException.asDataXException(HiveWriterErrorCode.Write_FILE_IO_ERROR, e);
        }
    }

    public List<String> getColumnNames(List<Configuration> columns){
        List<String> columnNames = Lists.newArrayList();
        for (Configuration eachColumnConf : columns) {
            columnNames.add(eachColumnConf.getString(Key.NAME));
        }
        return columnNames;
    }

    /**
     * 根据writer配置的字段类型，构建inspector
     * @param columns
     * @return
     */
    public List<ObjectInspector>  getColumnTypeInspectors(List<Configuration> columns){
        List<ObjectInspector>  columnTypeInspectors = Lists.newArrayList();
        for (Configuration eachColumnConf : columns) {
            SupportHiveDataType columnType = SupportHiveDataType.valueOf(eachColumnConf.getString(Key.TYPE).toUpperCase());
            ObjectInspector objectInspector = null;
            switch (columnType) {
                case TINYINT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Byte.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case SMALLINT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Short.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case INT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case BIGINT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case FLOAT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Float.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case DOUBLE:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Double.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case TIMESTAMP:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Timestamp.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case DATE:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Date.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case STRING:
                case VARCHAR:
                case CHAR:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case BOOLEAN:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                default:
                    throw DataXException
                            .asDataXException(
                                    HiveWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d]. 请修改表中该字段的类型或者不同步该字段.",
                                            eachColumnConf.getString(Key.NAME),
                                            eachColumnConf.getString(Key.TYPE)));
            }

            columnTypeInspectors.add(objectInspector);
        }
        return columnTypeInspectors;
    }

    public OrcSerde getOrcSerde(Configuration config){
        String fieldDelimiter = config.getString(Key.FIELD_DELIMITER);
        String compress = config.getString(Key.COMPRESS);
        String encoding = config.getString(Key.ENCODING);

        OrcSerde orcSerde = new OrcSerde();
        Properties properties = new Properties();
        properties.setProperty("orc.bloom.filter.columns", fieldDelimiter);
        properties.setProperty("orc.compress", compress);
        properties.setProperty("orc.encoding.strategy", encoding);

        orcSerde.initialize(conf, properties);
        return orcSerde;
    }

    public static MutablePair<List<Object>, Boolean> transportOneRecord(
            Record record,List<Configuration> columnsConfiguration,
            TaskPluginCollector taskPluginCollector){

        MutablePair<List<Object>, Boolean> transportResult = new MutablePair<List<Object>, Boolean>();
        transportResult.setRight(false);
        List<Object> recordList = Lists.newArrayList();
        int recordLength = record.getColumnNumber();
        if (0 != recordLength) {
            Column column;
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                //todo as method
                if (null != column.getRawData()) {
                    String rowData = column.getRawData().toString();
                    SupportHiveDataType columnType = SupportHiveDataType.valueOf(
                            columnsConfiguration.get(i).getString(Key.TYPE).toUpperCase());
                    //根据writer端类型配置做类型转换
                    try {
                        switch (columnType) {
                            case TINYINT:
                                recordList.add(Byte.valueOf(rowData));
                                break;
                            case SMALLINT:
                                recordList.add(Short.valueOf(rowData));
                                break;
                            case INT:
                                recordList.add(Integer.valueOf(rowData));
                                break;
                            case BIGINT:
                                recordList.add(column.asLong());
                                break;
                            case FLOAT:
                                recordList.add(Float.valueOf(rowData));
                                break;
                            case DOUBLE:
                                recordList.add(column.asDouble());
                                break;
                            case STRING:
                            case VARCHAR:
                            case CHAR:
                                recordList.add(column.asString());
                                break;
                            case BOOLEAN:
                                recordList.add(column.asBoolean());
                                break;
                            case DATE:
                                recordList.add(new java.sql.Date(column.asDate().getTime()));
                                break;
                            case TIMESTAMP:
                                recordList.add(new java.sql.Timestamp(column.asDate().getTime()));
                                break;
                            default:
                                throw DataXException
                                        .asDataXException(
                                                HiveWriterErrorCode.ILLEGAL_VALUE,
                                                String.format(
                                                        "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d]. 请修改表中该字段的类型或者不同步该字段.",
                                                        columnsConfiguration.get(i).getString(Key.NAME),
                                                        columnsConfiguration.get(i).getString(Key.TYPE)));
                        }
                    } catch (Exception e) {
                        // warn: 此处认为脏数据
                        String message = String.format(
                                "字段类型转换错误：您目标字段为[%s]类型，实际字段值为[%s].",
                                columnsConfiguration.get(i).getString(Key.TYPE), column.getRawData().toString());
                        taskPluginCollector.collectDirtyRecord(record, message);
                        transportResult.setRight(true);
                        break;
                    }
                }else {
                    // warn: it's all ok if nullFormat is null
                    recordList.add(null);
                }
            }
        }
        transportResult.setLeft(recordList);
        return transportResult;
    }

    public static String generateParquetSchemaFromColumnAndType(List<Configuration> columns) {
        Map<String, ColumnTypeUtil.DecimalInfo> decimalColInfo = new HashMap<>(16);
        ColumnTypeUtil.DecimalInfo PARQUET_DEFAULT_DECIMAL_INFO = new ColumnTypeUtil.DecimalInfo(10, 2);
        Types.MessageTypeBuilder typeBuilder = Types.buildMessage();
        for (Configuration column : columns) {
            String name = column.getString("name");
            String colType = column.getString("type");
            Validate.notNull(name, "column.name can't be null");
            Validate.notNull(colType, "column.type can't be null");
            switch (colType.toLowerCase()) {
                case "tinyint":
                case "smallint":
                case "int":
                    typeBuilder.optional(PrimitiveType.PrimitiveTypeName.INT32).named(name);
                    break;
                case "bigint":
                case "long":
                    typeBuilder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(name);
                    break;
                case "float":
                    typeBuilder.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named(name);
                    break;
                case "double":
                    typeBuilder.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(name);
                    break;
                case "binary":
                    typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(name);
                    break;
                case "char":
                case "varchar":
                case "string":
                    typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(name);
                    break;
                case "boolean":
                    typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(name);
                    break;
                case "timestamp":
                    typeBuilder.optional(PrimitiveType.PrimitiveTypeName.INT96).named(name);
                    break;
                case "date":
                    typeBuilder.optional(PrimitiveType.PrimitiveTypeName.INT32).as(OriginalType.DATE).named(name);
                    break;
                default:
                    if (ColumnTypeUtil.isDecimalType(colType)) {
                        ColumnTypeUtil.DecimalInfo decimalInfo = ColumnTypeUtil.getDecimalInfo(colType, PARQUET_DEFAULT_DECIMAL_INFO);
                        typeBuilder.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                                .as(OriginalType.DECIMAL)
                                .precision(decimalInfo.getPrecision())
                                .scale(decimalInfo.getScale())
                                .length(HdfsUtil.computeMinBytesForPrecision(decimalInfo.getPrecision()))
                                .named(name);

                        decimalColInfo.put(name, decimalInfo);
                    } else {
                        typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(name);
                    }
                    break;
            }
        }
        return typeBuilder.named("m").toString();
    }

    /**
     * 配置hdfs路径和文件的属性
     */
//    public boolean hdfsChangeOwner(String filePath, String ownerName, String groupName) {
//        boolean exit = false;
//        File pathStr = new File(filePath);
//        Path path = new Path(filePath);
//        Path parentPath = new Path(pathStr.getParent());
//        try {
//            if (fileSystem.exists(path)) {
//                fileSystem.setOwner(path, ownerName, groupName);
//                fileSystem.setOwner(parentPath, ownerName, groupName);
//                FileStatus[] fileStatuses = fileSystem.listStatus(path);
//                //一级目录1;
//                for (FileStatus fileStatus : fileStatuses) {
//                    Path subPath1 = fileStatus.getPath();
//                    fileSystem.setOwner(subPath1, ownerName, groupName);
//                    if (fileStatus.isDirectory()) {
//                        FileStatus[] subPath1FileStatuses = fileSystem.listStatus(subPath1);
//                        //二级目录2;
//                        for (FileStatus subPath1FileStatus : subPath1FileStatuses) {
//                            Path subPath2 = subPath1FileStatus.getPath();
//                            fileSystem.setOwner(subPath2, ownerName, groupName);
//                            if (subPath1FileStatus.isDirectory()){
//                                FileStatus[] subPath2FileStatuses = fileSystem.listStatus(subPath2);
//                                //三级目录3;
//                                for (FileStatus subPath2FileStatus : subPath2FileStatuses) {
//                                    Path subPath3 = subPath2FileStatus.getPath();
//                                    fileSystem.setOwner(subPath3, ownerName, groupName);
//                                }
//                            }
//                        }
//                    }
//                }
//
//                RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(path, true);
//                while(listFiles.hasNext()) {
//                    LocatedFileStatus file = listFiles.next();
//                    String owner = file.getOwner();
//                    if (!owner.equals(ownerName)) {
//                        Path p = file.getPath();
//                        fileSystem.setOwner(p, ownerName, groupName);
//                    }
//                }
//                exit = true;
//            } else {
//                LOG.info(String.format("文件路径[%s]不存在！", path));
//            }
//            LOG.info(String.format("文件路径[%s]所属用户为[%s], 所属用户组为[%s]！",
//                    path, ownerName, groupName));
//        } catch (IOException e) {
//            String message = String.format("文件路径[%s]属性时发生网络IO异常,请检查您的网络是否正常！",
//                    path);
//            LOG.error(message);
//            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
//        }
//        return exit;
//    }

    /**
     * 配置hdfs路径和文件的权限
     * @param filePath
     * @param ugoMode
     * @return
     */
//    public boolean hdfsChangeMode(String filePath, String ugoMode) {
//        boolean exit = false;
//        File pathStr = new File(filePath);
//        Path path = new Path(filePath);
//        Path parentPath = new Path(pathStr.getParent());
//        FsPermission fsPermission = null;
//        String ugoModeStr = null;
//
//        try {
//            if (StringUtils.isBlank(ugoMode)) {
//                fsPermission = new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE);
//                ugoModeStr = "rw-------";
//            }else {
//                if (ugoMode.equals("all") || ugoMode.equals("a")) {
//                    fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
//                    ugoModeStr = "rwwrwxrwx";
//                }else if (ugoMode.equals("read") || ugoMode.equals("r")) {
//                    fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.READ);
//                    ugoModeStr = "rw-r--r--";
//                }else if (ugoMode.equals("write") || ugoMode.equals("w")) {
//                    fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.WRITE, FsAction.WRITE);
//                    ugoModeStr = "rw--w--w-";
//                }else if (ugoMode.equals("execute") || ugoMode.equals("x")) {
//                    fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.EXECUTE, FsAction.EXECUTE);
//                    ugoModeStr = "rw---x--x";
//                }else if (ugoMode.equals("read_write") || ugoMode.equals("rw")) {
//                    fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE);
//                    ugoModeStr = "rw-rw-rw-";
//                }else if (ugoMode.equals("read_execute") || ugoMode.equals("rx")) {
//                    fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE);
//                    ugoModeStr = "rw-r-xr-x";
//                }else if (ugoMode.equals("write_execute") || ugoMode.equals("wx")) {
//                    fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.WRITE_EXECUTE, FsAction.WRITE_EXECUTE);
//                    ugoModeStr = "rw---wx-wx";
//                }else if (ugoMode.equals("none")) {
//                    fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);
//                    ugoModeStr = "rw-------";
//                }
//            }
//            fileSystem.setPermission(path, fsPermission);
//            fileSystem.setPermission(parentPath, fsPermission);
//            FileStatus[] fileStatuses = fileSystem.listStatus(path);
//            for (FileStatus fileStatus : fileStatuses) {
//                Path subPath1 = fileStatus.getPath();
//                fileSystem.setPermission(subPath1, fsPermission);
//                if (fileStatus.isDirectory()) {
//                    FileStatus[] subPath1FileStatuses = fileSystem.listStatus(subPath1);
//                    for (FileStatus subPath1FileStatus : subPath1FileStatuses) {
//                        Path subPath2 = subPath1FileStatus.getPath();
//                        fileSystem.setPermission(subPath2, fsPermission);
//                        if (subPath1FileStatus.isDirectory()){
//                            FileStatus[] subPath2FileStatuses = fileSystem.listStatus(subPath2);
//                            for (FileStatus subPath2FileStatus : subPath2FileStatuses) {
//                                Path subPath3 = subPath2FileStatus.getPath();
//                                fileSystem.setPermission(subPath3, fsPermission);
//                            }
//                        }
//                    }
//                }
//            }
//
//            RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(path, true);
//            while(listFiles.hasNext()) {
//                LocatedFileStatus file = listFiles.next();
//                Path p = file.getPath();
//                // rw-r--r--
//                FsPermission pPermession = file.getPermission();
//                if (!pPermession.equals(ugoModeStr)) {
//                    fileSystem.setPermission(p, fsPermission);
//                }
//            }
//            exit = true;
//            LOG.info(String.format("文件路径[%s]的ugo权限为[%s]！", parentPath, ugoMode));
//        } catch (Exception e) {
//            String message = String.format("文件路径[%s]权限时发生网络IO异常,请检查您的网络是否正常！",
//                    parentPath);
//            LOG.error(message);
//            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
//        }
//        return exit;
//    }

    /**
     * 在hdfs上创建目录
     *
     * @param filePath 需要创建的目录
     * @return boolean
     */
    public boolean createPath(String filePath, String ownerName, String groupName, String ugoMode) {
        Path path = new Path(filePath);
        FsPermission fsPermission = getFsPermission(ugoMode);
        boolean exit = false;
        try {
            if (!fileSystem.exists(path)) {
                exit = fileSystem.mkdirs(path);

            }
            fileSystem.setOwner(path, ownerName, groupName);
            fileSystem.setPermission(path, fsPermission);
        } catch (IOException e) {
            String message = String.format("创建hdfs路径[%s]时发生网络IO异常,请检查您的网络是否正常！", filePath);
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
        return exit;
    }

    /**
     * 将HDFS文件load到hive表中
     * @param path
     * @param url
     * @param tableType
     * @param dbName
     * @param tableName
     * @param parColumns
     * @return
     */
    public boolean loadData2Hive(String path, String url, String userName, String password, String tableType, String dbName,
                                 String tableName, List<Configuration> parColumns) {
        String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
//        String CONNECTION_URL = "jdbc:hive2://server-13:10000/default;auth=noSasl";
//        String username = "hive";
//        String password = "123456";
        Connection conn = null;
        boolean exist = false;

        try {
            Class.forName(JDBC_DRIVER);
            conn = (Connection) DriverManager.getConnection(url, userName, password);
            Statement stmt = conn.createStatement();
            ArrayList<String> parColumnsArr = new ArrayList<>();
            String parColumnsNameValueStr = "";
            String hiveLoadSqlStr = "";

            if (tableType.equals("partition")) {
                if (parColumns.size() > 0) {
                    for (Configuration eachParColumn : parColumns) {
                        String eachParColumnName = eachParColumn.getNecessaryValue(Key.NAME, HiveWriterErrorCode.COLUMN_REQUIRED_VALUE);
                        String eachParColumnType = eachParColumn.getNecessaryValue(Key.TYPE, HiveWriterErrorCode.COLUMN_REQUIRED_VALUE);
                        String eachParColumnValue = eachParColumn.getNecessaryValue(Key.VALUE, HiveWriterErrorCode.COLUMN_REQUIRED_VALUE);
                        parColumnsArr.add(eachParColumnName + "='" + eachParColumnValue + "'");
                    }
                    parColumnsNameValueStr = StringUtils.strip(parColumnsArr.toString(), "[]");
                    hiveLoadSqlStr = "load data inpath '" + path + "'" + " into table " + dbName + "." + tableName
                            + " partition(" + parColumnsNameValueStr + ")";
                }
            }else if (tableType.equals("normal")){
                hiveLoadSqlStr = "load data inpath '" + path + "'" + " into table " + dbName + "." + tableName;
            }
            LOG.info("generate load data sql:\n{}", hiveLoadSqlStr);

            stmt.execute(hiveLoadSqlStr);
            exist = true;
            LOG.info(String.format("将hdfs路径[%s]数据加载到Hive表[%s]成功!", path, dbName + "." + tableName));
        }catch (Exception e) {
            String message = String.format("将hdfs路径[%s]数据加载到Hive表[%s]失败!", path, dbName + "." + tableName);
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
//        catch (SQLException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
        }finally {
            // 关闭rs、ps和con
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return exist;
    }

    /**
     * 自动创建Hive表
     * @param path
     * @param url
     * @param userName
     * @param password
     * @param tableType
     * @param dbName
     * @param tableName
     * @param columns
     * @param parColumns
     * @return
     */
    public boolean createHiveTable(String path, String url, String userName, String password, String tableType, String dbName,
                                   String tableName, List<Configuration> columns, List<Configuration> parColumns) {
//            SQL建表语句
//            CREATE EXTERNAL TABLE IF NOT EXISTS ods_user_info(
//                `id` STRING COMMENT '编号',
//                `name` STRING COMMENT '名称',
//                `address` STRING COMMENT '地址'
//            ) COMMENT '用户表'
//            PARTITIONED BY (`dt` STRING)
//            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
//            COLLECTION ITEMS TERMINATED BY ','
//            MAP KEYS TERMINATED BY ':'
//            stored as textfile
//            NULL DEFINED AS ''
//            LOCATION '/hive/test/ods/ods_user_info/';

//        "column": [ { "type": "bigint", "name": "id" }, { "type": "bigint", "name": "spu_id" } ]
        String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
//        String CONNECTION_URL = "jdbc:hive2://server-13:10000/default;auth=noSasl";
//        String CONNECTION_URL = url;
        Connection conn = null;
        boolean exit = false;
//        columns: [{"name":"id","type":"bigint"}, {"name":"spu_id","type":"bigint"}, {"name":"price","type":"string"}, {"name":"sku_name","type":"string"}, {"name":"sku_desc","type":"string"}, {"name":"weight","type":"string"}, {"name":"tm_id","type":"bigint"}, {"name":"category3_id","type":"bigint"}, {"name":"sku_default_img","type":"string"}, {"name":"is_sale","type":"bigint"}, {"name":"create_time","type":"string"}]
        //表字段
        String columnsStr = null;
        try {
            List<String> columnsArr = new ArrayList<>();
            columnsStr = "";
            Iterator<Configuration> iterator = columns.iterator();
            while (iterator.hasNext()) {
                // next: {"name":"id","type":"bigint"}
                Configuration next = iterator.next();
                JSONObject jsonobject = JSONObject.parseObject(next.toString());
                String colname = jsonobject.getString("name");
                String coltype = jsonobject.getString("type");
                String columnStr = "`" + colname + "`" + " " + coltype + " COMMENT '" + colname + "'";
                columnsArr.add(columnStr);
            }
            columnsStr = StringUtils.strip(columnsArr.toString(), "[]");
            LOG.info(String.format("您配置的Hive表字段信息为：[%s]!", columnsStr));
        } catch (Exception e) {
            String message = String.format("您配置的Hive表字段信息为：[%s], 是不合法的配置信息，请检查！", columns.toString());
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }

        //分区字段
        String parColumnsStr = "";
        try {
            List<String> parColumnsArr = new ArrayList<>();
            if (tableType.equals("partition")) {
                for (Configuration parcolumn : parColumns) {
                    String eachParColumnName = parcolumn.getNecessaryValue(Key.NAME, HiveWriterErrorCode.COLUMN_REQUIRED_VALUE);
                    String eachParColumnType = parcolumn.getNecessaryValue(Key.TYPE, HiveWriterErrorCode.COLUMN_REQUIRED_VALUE);
                    String parColumnStr = "`" + eachParColumnName + "`" + " " + eachParColumnType;
                    parColumnsArr.add(parColumnStr);
                }
                parColumnsStr = StringUtils.strip(parColumnsArr.toString(), "[]");
                LOG.info(String.format("您配置的是Hive分区表，分区字段信息为：[%s]!", parColumnsStr));
            }
        } catch (Exception e) {
            String message = String.format("您配置的是Hive分区表，分区字段信息为：[%s], 是不合法的分区配置信息，请检查！", parColumns.toString());
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }

        //建表语句
        String createTableSql = "";
//            CREATE EXTERNAL TABLE IF NOT EXISTS sku_info(`id` STRING COMMENT '编号', `address` STRING COMMENT '地址' ) COMMENT 'sku_info表' PARTITIONED BY (`2022-11-14`) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':' stored as textfile NULL DEFINED AS '' LOCATION 'null'
//            CREATE EXTERNAL TABLE IF NOT EXISTS sku_info(
//              `id` STRING COMMENT '编号',
//              `address` STRING COMMENT '地址'
//            ) COMMENT 'sku_info表'
//            PARTITIONED BY (`2022-11-14`)
//            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
//            COLLECTION ITEMS TERMINATED BY ','
//            MAP KEYS TERMINATED BY ':'
//            stored as textfile
//            NULL DEFINED AS ''
//            LOCATION 'null'
        try {
            if (tableType.equals("partition")) {
                createTableSql = "CREATE EXTERNAL TABLE IF NOT EXISTS " + dbName + "." + tableName + "(" +
                        columnsStr +
                        ") COMMENT '" + tableName + "_table' " +
                        "PARTITIONED BY (" + parColumnsStr + ") " +
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' " +
                        "COLLECTION ITEMS TERMINATED BY ',' " +
                        "MAP KEYS TERMINATED BY ':' " +
                        "LINES TERMINATED BY '\\n' " +
                        "stored as textfile " +
//                        "NULL DEFINED AS '' " +
                        "LOCATION '" + path + "'";

            } else if (tableType.equals("normal")) {
                createTableSql = "CREATE EXTERNAL TABLE IF NOT EXISTS " + dbName + "." + tableName + "(" +
                        columnsStr +
                        ") COMMENT '" + tableName + "_table' " +
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' " +
                        "COLLECTION ITEMS TERMINATED BY ',' " +
                        "MAP KEYS TERMINATED BY ':' " +
                        "LINES TERMINATED BY '\\n' " +
                        "stored as textfile " +
//                        "NULL DEFINED AS '' " +
                        "LOCATION '" + path + "'";
            }
            LOG.info(String.format("您配置的Hive建表SQL为：[%s]", createTableSql));
        } catch (Exception e) {
            e.printStackTrace();
        }
        //执行建表
        try {
            Class.forName(JDBC_DRIVER);
            conn = (Connection) DriverManager.getConnection(url, userName, password);
            Statement stmt = conn.createStatement();
            stmt.execute(createTableSql);
            exit = true;
            LOG.info(String.format("您配置的Hive表[%s]，自动创建成功！", dbName + "." + tableName));
        } catch (Exception e) {
            String message = String.format("您配置的Hive表[%s]，自动创建失败！", dbName + "." + tableName);
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
//        catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        } catch (SQLException e) {
//            e.printStackTrace();
        }finally {
            // 关闭rs、ps和con
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return exit;
    }

    /**
     * Hive表是否存在
     * @param url
     * @param userName
     * @param password
     * @param dbName
     * @param tableName
     * @return
     */
    public boolean isHiveTableExists(String url, String userName, String password,
                                     String dbName, String tableName) {
        String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
        Connection conn = null;
        boolean exit = false;
        // show tables in test like 'sku_info'
        String searchTableSql = "show tables in " + dbName + " like '"  + tableName + "'";
        try {
            Class.forName(JDBC_DRIVER);
            conn = (Connection) DriverManager.getConnection(url, userName, password);
            Statement stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery(searchTableSql);
            ArrayList<String> searchTablesArr = new ArrayList<>();
            while (res.next()) {
                searchTablesArr.add(res.getString(1));
            }
            if (searchTablesArr.contains(tableName)) {
                exit = true;
                LOG.info(String.format("您配置的Hive表[%s]，存在！", dbName + "." + tableName));
            }else {
                LOG.info(String.format("您配置的Hive表[%s]，不存在！", dbName + "." + tableName));
            }
        } catch (Exception e) {
            String message = String.format("您执行查询Hive表失败！", searchTableSql);
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
//        catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        } catch (SQLException e) {
//            e.printStackTrace();
        }finally {
            // 关闭rs、ps和con
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return exit;
    }

    /**
     * 获取Hive已存在表的location
     * @param url
     * @param userName
     * @param password
     * @param dbName
     * @param tableName
     * @return hdfs://192.168.0.45:8020/dev/test/sku_info
     */
    public String getHiveTableLocation (String url, String userName, String password,
                                        String dbName, String tableName) {
        String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
        Connection conn = null;
        String hiveLocation = "";
        Integer hiveLocationIndex = null;
        // show create table dev.sku_info;
        String showCreateTableSql = "show create table " + dbName + "." + tableName;
//
//+----------------------------------------------------+
//|                   createtab_stmt                   |
//+----------------------------------------------------+
//| CREATE EXTERNAL TABLE `dev.sku_info`(              |
//|   `id` string COMMENT 'skuId',                     |
//|   `spu_id` string COMMENT 'spuid',                 |
//|   `price` decimal(16,2) COMMENT '??',              |
//|   `sku_name` string COMMENT '????',                |
//|   `sku_desc` string COMMENT '????',                |
//|   `weight` decimal(16,2) COMMENT '??',             |
//|   `tm_id` string COMMENT '??id',                   |
//|   `category3_id` string COMMENT '??id',            |
//|   `sku_default_igm` string COMMENT '??????',       |
//|   `is_sale` string COMMENT '????',                 |
//|   `create_time` string COMMENT '????')             |
//| COMMENT 'SKU???'                                   |
//| PARTITIONED BY (                                   |
//|   `dt` string)                                     |
//| ROW FORMAT SERDE                                   |
//|   'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'  |
//| WITH SERDEPROPERTIES (                             |
//|   'field.delim'='\t',                              |
//|   'serialization.format'='\t',                     |
//|   'serialization.null.format'='')                  |
//| STORED AS INPUTFORMAT                              |
//|   'org.apache.hadoop.mapred.TextInputFormat'       |
//| OUTPUTFORMAT                                       |
//|   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' |
//| LOCATION                                           |
//|   'hdfs://server02.novalocal:8020/dev/test/sku_info' |
//| TBLPROPERTIES (                                    |
//|   'bucketing_version'='2',                         |
//|   'discover.partitions'='true',                    |
//|   'transient_lastDdlTime'='1668008672')            |
//+----------------------------------------------------+
        try {
            Class.forName(JDBC_DRIVER);
            conn = (Connection) DriverManager.getConnection(url, userName, password);
            Statement stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery(showCreateTableSql);

            ArrayList<String> resultArr = new ArrayList<>();
            while (res.next()) {
                resultArr.add(res.getString(1));
            }
            for (int i = 0; i < resultArr.size(); i++) {
               if (resultArr.get(i).toString().toLowerCase().equals("location")) {
                   hiveLocationIndex = i + 1;
               }
            }
            hiveLocation = resultArr.get(hiveLocationIndex).toString();

//            if (StringUtils.isNotBlank(hiveLocation)) {
//                LOG.info(String.format("您配置的Hive表: [%s] 的location为：且dfs目录: [%s] 目录存在！", dbName + "." + tableName, hiveLocation));
//            }else {
//                LOG.info(String.format("您配置的Hive表: [%s] 已损坏不可用，因为location： [%s] 目录不存在！", dbName + "." + tableName, hiveLocation));
//            }
        }catch (Exception e) {
            String message = String.format("您执行获取Hive表Location失败，请检查配置！", showCreateTableSql);
            LOG.error(message);
            throw DataXException.asDataXException(HiveWriterErrorCode.CONNECT_HDFS_IO_ERROR, e);
//        catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        } catch (SQLException e) {
//            e.printStackTrace();
        }finally {
            // 关闭rs、ps和con
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return hiveLocation;
    }

    public boolean hdfsCurrentParentPathPermession(String defaultFS, String filePath, String ownerName, String groupName, String ugoMode){
        boolean exit = false;
        FsPermission fsPermission = null;
        File filepath = new File(filePath);
        Path path = new Path(filePath);
        String parentpath = filepath.getParent();
        Path parentPath = new Path(parentpath);

        fsPermission = getFsPermission(ugoMode);

        try {
            if (isPathexists(filePath)){
                if (isPathDir(filePath)) {
                    fileSystem.setOwner(path, ownerName, groupName);
                    fileSystem.setPermission(path, fsPermission);
                }
                if (isPathexists(parentpath)) {
                    fileSystem.setOwner(parentPath, ownerName, groupName);
                    fileSystem.setPermission(parentPath, fsPermission);

                }
            }
            exit = true;
//            LOG.info(String.format("配置当前hdfs目录: [%s]和其父目录: [%s] 的所属者和权限[%s] 成功！", filepath, parentpath, ownerName+":"+fsPermission));
        } catch (IOException e) {
            LOG.info(String.format("配置当前hdfs目录: [%s]和其父目录: [%s] 的所属者和权限[%s] 失败！", filepath, parentpath, ownerName+":"+fsPermission));
            e.printStackTrace();
        }

        return exit;
    }
//    public ArrayList getSubPath(Path path) {
//        ArrayList<String> pathArr = new ArrayList<>();
//        try {
//            if (fileSystem != null && path != null) {
//                //获取文件列表
//                FileStatus[] files = fileSystem.listStatus(path);
//                Path[] paths = FileUtil.stat2Paths(files);
//                for (Path p : paths) {
//                    pathArr.add(p.toString());
//                }
//                System.out.println("###" + pathArr.toString());
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return pathArr;
//    }
//
//    public HashMap getRecursionSubPath(Path path) {
//        HashMap<String, ArrayList> recursionPathHashMap = new HashMap<>();
//        ArrayList<String> recursionPathHashMapValue = new ArrayList<>();
//        try {
//            if (fileSystem != null && path != null) {
//                //获取文件列表
//                FileStatus[] files = fileSystem.listStatus(path);
//                //展示文件信息
//                for (int i = 0; i < files.length; i++) {
//                    //生成Key
//                    String recursionPathHashMapKey = files[i].getPath().toString();
//                    if (files[i].isDirectory()) {
//                        System.out.println(">>>" + files[i].getPath()+ ", dir owner:" + files[i].getOwner());
//                        //递归调用获取子目录生成value
//                        recursionPathHashMapValue = getSubPath(files[i].getPath());
//                    }
//                    recursionPathHashMap.put(recursionPathHashMapKey, recursionPathHashMapValue);
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return recursionPathHashMap;
//    }
//
//    public ArrayList getRecursionPath(Path path) {
//        try {
//            FileStatus[] fileStatuses = fileSystem.listStatus(path);
//            if (fileStatuses != null && fileStatuses.length > 0) {
//                for (FileStatus fileStatus : fileStatuses) {
//                    if (fileStatus.isDirectory()) {
//                        String subPath = fileStatus.getPath().toString();
//
//                        recursionPathsArr.add(subPath);
//
////                        System.out.println("========================" + subPath);
//                        Path recursionPath = new Path(subPath);
//                        getRecursionPath(recursionPath);
//                    }
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return recursionPathsArr;
//    }

    public boolean hdfsRecursionPermession(String defaultFS, String filePath, String ownerName, String groupName, String ugoMode) {
        boolean exit = false;
        String defaultFSStr = StringUtils.strip(defaultFS.toString(), "[]");
        FsPermission fsPermission = null;
        String ugoModeStr = null;
//        File pathStr = new File(filePath);
        Path path = new Path(filePath);
        fsPermission = getFsPermission(ugoMode);

        try {
            if (fileSystem.exists(path)) {
                fileSystem.setOwner(path, ownerName, groupName);
                fileSystem.setPermission(path, fsPermission);
            }
            FileStatus[] fileStatuses = fileSystem.listStatus(path);
            if (fileStatuses != null && fileStatuses.length > 0) {
                for (FileStatus fileStatus : fileStatuses) {
                    if (fileStatus.isDirectory()) {
                        String s = fileStatus.getPath().toString();
                        String subPath = s.split(defaultFSStr)[1];
                        fileSystem.setOwner(new Path(subPath), ownerName, groupName);
                        fileSystem.setPermission(new Path(subPath), fsPermission);
                        hdfsRecursionPermession(defaultFS, subPath, ownerName, groupName, ugoMode);
                    } else if (fileStatus.isFile()) {
                        String s = fileStatus.getPath().toString();
                        String subFile = s.split(defaultFSStr)[1];
                        fileSystem.setOwner(new Path(subFile), ownerName, groupName);
                        fileSystem.setPermission(new Path(subFile), fsPermission);
                    }
                }
            }
            exit = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return exit;
    }

    public FsPermission getFsPermission(String ugoMode) {
        String ugoModeStr;
        FsPermission fsPermission = null;
        if (StringUtils.isBlank(ugoMode)) {
            fsPermission = new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE);
            ugoModeStr = "rw-------";
        }else {
            if (ugoMode.equals("all") || ugoMode.equals("a")) {
                fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
                ugoModeStr = "rwwrwxrwx";
            }else if (ugoMode.equals("read") || ugoMode.equals("r")) {
                fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.READ);
                ugoModeStr = "rw-r--r--";
            }else if (ugoMode.equals("write") || ugoMode.equals("w")) {
                fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.WRITE, FsAction.WRITE);
                ugoModeStr = "rw--w--w-";
            }else if (ugoMode.equals("execute") || ugoMode.equals("x")) {
                fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.EXECUTE, FsAction.EXECUTE);
                ugoModeStr = "rw---x--x";
            }else if (ugoMode.equals("read_write") || ugoMode.equals("rw")) {
                fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE);
                ugoModeStr = "rw-rw-rw-";
            }else if (ugoMode.equals("read_execute") || ugoMode.equals("rx")) {
                fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE);
                ugoModeStr = "rw-r-xr-x";
            }else if (ugoMode.equals("write_execute") || ugoMode.equals("wx")) {
                fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.WRITE_EXECUTE, FsAction.WRITE_EXECUTE);
                ugoModeStr = "rw---wx-wx";
            }else if (ugoMode.equals("none")) {
                fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);
                ugoModeStr = "rw-------";
            }
        }
        return fsPermission;
    }

//    public boolean hdfsRecursionChown(String filePath, String ownerName, String groupName, String ugoMode) {
//        boolean exit = false;
//        FsPermission fsPermission = null;
//        String ugoModeStr = null;
//        File pathStr = new File(filePath);
//        Path path = new Path(filePath);
//        Path parentPath = new Path(pathStr.getParent());
//
//        if (StringUtils.isBlank(ugoMode)) {
//            fsPermission = new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE);
//            ugoModeStr = "rw-------";
//        }else {
//            if (ugoMode.equals("all") || ugoMode.equals("a")) {
//                fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
//                ugoModeStr = "rwwrwxrwx";
//            }else if (ugoMode.equals("read") || ugoMode.equals("r")) {
//                fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.READ);
//                ugoModeStr = "rw-r--r--";
//            }else if (ugoMode.equals("write") || ugoMode.equals("w")) {
//                fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.WRITE, FsAction.WRITE);
//                ugoModeStr = "rw--w--w-";
//            }else if (ugoMode.equals("execute") || ugoMode.equals("x")) {
//                fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.EXECUTE, FsAction.EXECUTE);
//                ugoModeStr = "rw---x--x";
//            }else if (ugoMode.equals("read_write") || ugoMode.equals("rw")) {
//                fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE);
//                ugoModeStr = "rw-rw-rw-";
//            }else if (ugoMode.equals("read_execute") || ugoMode.equals("rx")) {
//                fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE);
//                ugoModeStr = "rw-r-xr-x";
//            }else if (ugoMode.equals("write_execute") || ugoMode.equals("wx")) {
//                fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.WRITE_EXECUTE, FsAction.WRITE_EXECUTE);
//                ugoModeStr = "rw---wx-wx";
//            }else if (ugoMode.equals("none")) {
//                fsPermission = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);
//                ugoModeStr = "rw-------";
//            }
//        }
//
//        //所有者
//        try {
//            if (fileSystem.exists(path)) {
//                fileSystem.setOwner(path, ownerName, groupName);
//                fileSystem.setPermission(path, fsPermission);
//                if (fileSystem.exists(parentPath)) {
//                    fileSystem.setOwner(parentPath, ownerName, groupName);
//                    fileSystem.setPermission(parentPath, fsPermission);
//                }
//                HashMap recursionSubPathMap = getRecursionSubPath(path);
//
//                Iterator<Map.Entry<String, ArrayList>> entries = recursionSubPathMap.entrySet().iterator();
//                while (entries.hasNext()) {
//                    Map.Entry<String, ArrayList> entry = entries.next();
//                    String entryKey = entry.getKey();
//                    fileSystem.setOwner(new Path(entryKey), ownerName, groupName);
//                    fileSystem.setPermission(new Path(entryKey), fsPermission);
//                    ArrayList entryValue = entry.getValue();
//                    if (entryValue.size() > 0) {
//                        for (int i = 0; i < entryValue.size(); i++) {
//                            Path recursionSubPath = new Path((String) entryValue.get(i));
//                            fileSystem.setOwner(recursionSubPath, ownerName, groupName);
//                            fileSystem.setPermission(recursionSubPath, fsPermission);
//                        }
//                    }
//                }
//            }
//            exit = true;
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return exit;
//    }

}
