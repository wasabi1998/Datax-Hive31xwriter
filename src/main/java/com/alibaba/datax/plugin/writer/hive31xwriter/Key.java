package com.alibaba.datax.plugin.writer.hive31xwriter;

import org.apache.hadoop.tools.CopyListing;

/**
 * Created by shf on 15/10/8.
 */
public class Key {
    //must have
    public static final String URL = "url";
    //must have
    public static final String USERANAME = "userName";
    //must have
    public static final String PASSWORD = "password";
    //must have
    public static final String DB_NAME = "dbName";
    //must have
    public static final String TABLE_NAME = "tableName";
    //must have
    public static final String TABLE_Type = "tableType";
    //must have
    public static final String PAR_COLUMN = "parColumn";
    //partition value
    public static final String VALUE = "value";
    //must have
    public static final String LOCATION_PATH = "locationPath";

    // must have
    public static final String PATH = "path";
    //must have
    public final static String DEFAULT_FS = "defaultFS";
    //must have
    public final static String FILE_TYPE = "fileType";
    // must have
    public static final String FILE_NAME = "fileName";
    // must have for column
    public static final String COLUMN = "column";
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String DATE_FORMAT = "dateFormat";
    // must have
    public static final String WRITE_MODE = "writeMode";
    // must have
    public static final String FIELD_DELIMITER = "fieldDelimiter";
    // not must, default UTF-8
    public static final String ENCODING = "encoding";
    // not must, default no compress
    public static final String COMPRESS = "compress";
    // not must, not default \N
    public static final String NULL_FORMAT = "nullFormat";
    // Kerberos
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";
    // hadoop config
    public static final String HADOOP_CONFIG = "hadoopConfig";

    // useOldRawDataTransf
    public final static String PARQUET_FILE_USE_RAW_DATA_TRANSF = "useRawDataTransf";

    public final static String DATAX_PARQUET_MODE = "dataxParquetMode";

    // hdfs username 默认值 admin
    public final static String HDFS_USERNAME = "hdfsUsername";
    // option
    public static final String HDFS_GROUPNAME = "hdfsGroupname";
    // option
    public static final String SUPERUSER = "superUser";
    // option
    public static final String SUPERGROUP = "superGroup";
    // option
    public static final String UGOMODE = "ugoMode";

    public static final String PROTECTION = "protection";

    public static final String PARQUET_SCHEMA = "parquetSchema";
    public static final String PARQUET_MERGE_RESULT = "parquetMergeResult";
}
