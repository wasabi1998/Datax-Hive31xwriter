package com.alibaba.datax.plugin.writer.hive31xwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.writer.Constant;
import com.google.common.collect.Sets;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.thrift.protocol.TMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @ProjectName： DataX-datax_v202210-modifybyxiaom
 * @packageName: com.alibaba.datax.plugin.writer.hive31xwriter
 * @className: HiveWriter
 * @authorName: xiaom
 * @createDate: 2022/11/10 15:20
 * @version: 1.0
 * @description:
 */
public class HiveWriter extends Writer {
	public static class Job extends Writer.Job {
		private static final Logger LOG = LoggerFactory.getLogger(HiveWriter.Job.class);

		private Configuration writerSliceConfig = null;

		private String hdfsUsername;
		private String hdfsGroupname;
		private String superUser;
		private String superGroup;
		private String ugoMode;

		private String url;
		private String userName;
		private String password;
		private String dbName;
		private String tableName;
		private String tableType;
		private List<Configuration> parColumns;
		private String locationPath;

		private String defaultFS;
		private String path;
		private String fileType;
		private String fileName;
		private List<Configuration> columns;
		private String writeMode;
		private String fieldDelimiter;
		private String compress;
		private String encoding;

		private HashSet<String> tmpFiles = new HashSet<String>();//临时文件全路径
		private HashSet<String> endFiles = new HashSet<String>();//最终文件全路径

		private HiveHelper hdfsHelper = null;

		@Override
		public void init() {
			this.writerSliceConfig = this.getPluginJobConf();
			this.validateParameter();

			//创建textfile存储
			hdfsHelper = new HiveHelper();

			hdfsHelper.getFileSystem(defaultFS, this.writerSliceConfig);
		}

		private void validateParameter() {
			// hdfsUsername
			this.hdfsUsername = this.writerSliceConfig.getUnnecessaryValue(Key.HDFS_USERNAME, "hdfs", HiveWriterErrorCode.REQUIRED_VALUE);
			// hdfsGroupname
			this.hdfsGroupname = this.writerSliceConfig.getUnnecessaryValue(Key.HDFS_GROUPNAME, "hadoop", HiveWriterErrorCode.REQUIRED_VALUE);
			// superUser
			this.superUser = this.writerSliceConfig.getUnnecessaryValue(Key.SUPERUSER, "hdfs", HiveWriterErrorCode.REQUIRED_VALUE);
			// superGroup
			this.superGroup = this.writerSliceConfig.getUnnecessaryValue(Key.SUPERGROUP, "supergroup", HiveWriterErrorCode.REQUIRED_VALUE);
			// ugoMode
			this.ugoMode = this.writerSliceConfig.getUnnecessaryValue(Key.UGOMODE, "755", HiveWriterErrorCode.REQUIRED_VALUE);
			//url
			this.url = this.writerSliceConfig.getNecessaryValue(Key.URL, HiveWriterErrorCode.REQUIRED_VALUE);
			//userName
			this.userName = this.writerSliceConfig.getNecessaryValue(Key.USERANAME, HiveWriterErrorCode.REQUIRED_VALUE);
			//password
			this.password = this.writerSliceConfig.getNecessaryValue(Key.PASSWORD, HiveWriterErrorCode.REQUIRED_VALUE);
			//dbName
			this.dbName = this.writerSliceConfig.getNecessaryValue(Key.DB_NAME, HiveWriterErrorCode.REQUIRED_VALUE);
			//tableName
			this.tableName = this.writerSliceConfig.getNecessaryValue(Key.TABLE_NAME, HiveWriterErrorCode.REQUIRED_VALUE);
			//tableType
			this.tableType = this.writerSliceConfig.getNecessaryValue(Key.TABLE_Type, HiveWriterErrorCode.REQUIRED_VALUE);

			// defaultFs
			this.defaultFS = this.writerSliceConfig.getNecessaryValue(Key.DEFAULT_FS, HiveWriterErrorCode.REQUIRED_VALUE);
			//fileType check
			this.fileType = this.writerSliceConfig.getNecessaryValue(Key.FILE_TYPE, HiveWriterErrorCode.REQUIRED_VALUE);
			if( !fileType.equalsIgnoreCase("ORC") && !fileType.equalsIgnoreCase("TEXT") && !fileType.equalsIgnoreCase("PARQUET")){
//                String message = "HdfsWriter插件目前只支持ORC和TEXT两种格式的文件,请将filetype选项的值配置为ORC或者TEXT";
				String message = "HdfsWriter插件目前只支持ORC和TEXT和PARQUET三种格式的文件,请将filetype选项的值配置为ORC或者TEXT或者PARQUET";
				throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE, message);
			}
			//path
			this.path = this.writerSliceConfig.getNecessaryValue(Key.PATH, HiveWriterErrorCode.REQUIRED_VALUE);
			if(!path.startsWith("/")){
				String message = String.format("请检查参数path:[%s],需要配置为绝对路径", path);
				LOG.error(message);
				throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE, message);
			}else if(path.contains("*") || path.contains("?")){
				String message = String.format("请检查参数path:[%s],不能包含*,?等特殊字符", path);
				LOG.error(message);
				throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE, message);
			}
			// locationPath
			this.locationPath = this.writerSliceConfig.getNecessaryValue(Key.LOCATION_PATH, HiveWriterErrorCode.REQUIRED_VALUE);
			if(!locationPath.startsWith("/")){
				String message = String.format("请检查参数localtionPath:[%s],需要配置为绝对路径", locationPath);
				LOG.error(message);
				throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE, message);
			}else if(locationPath.contains("*") || locationPath.contains("?")){
				String message = String.format("请检查参数locationPath:[%s],不能包含*,?等特殊字符", locationPath);
				LOG.error(message);
				throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE, message);
			}

			//fileName
			this.fileName = this.writerSliceConfig.getNecessaryValue(Key.FILE_NAME, HiveWriterErrorCode.REQUIRED_VALUE);
			//columns check
			this.columns = this.writerSliceConfig.getListConfiguration(Key.COLUMN);
			if (null == columns || columns.size() == 0) {
				throw DataXException.asDataXException(HiveWriterErrorCode.REQUIRED_VALUE, "您需要指定 columns");
			}else{
				for (Configuration eachColumnConf : columns) {
					eachColumnConf.getNecessaryValue(Key.NAME, HiveWriterErrorCode.COLUMN_REQUIRED_VALUE);
					eachColumnConf.getNecessaryValue(Key.TYPE, HiveWriterErrorCode.COLUMN_REQUIRED_VALUE);
				}
			}
			// parColumns check
			this.parColumns = this.writerSliceConfig.getListConfiguration(Key.PAR_COLUMN);
			if (tableType.equals("partition")) {
				if (null == parColumns || parColumns.size() == 0) {
					throw DataXException.asDataXException(HiveWriterErrorCode.REQUIRED_VALUE, "您需要指定分区配置 parColumns");
				}else{
					for (Configuration eachParColumnConf : parColumns) {
						eachParColumnConf.getNecessaryValue(Key.NAME, HiveWriterErrorCode.COLUMN_REQUIRED_VALUE);
						eachParColumnConf.getNecessaryValue(Key.TYPE, HiveWriterErrorCode.COLUMN_REQUIRED_VALUE);
						eachParColumnConf.getNecessaryValue(Key.VALUE, HiveWriterErrorCode.COLUMN_REQUIRED_VALUE);
					}
				}
			}
			//writeMode check
			this.writeMode = this.writerSliceConfig.getNecessaryValue(Key.WRITE_MODE, HiveWriterErrorCode.REQUIRED_VALUE);
			writeMode = writeMode.toLowerCase().trim();
			Set<String> supportedWriteModes = Sets.newHashSet("append", "nonconflict", "truncate");
			if (!supportedWriteModes.contains(writeMode)) {
				throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE,
						String.format("仅支持append, nonConflict, truncate三种模式, 不支持您配置的 writeMode 模式 : [%s]",
								writeMode));
			}
			this.writerSliceConfig.set(Key.WRITE_MODE, writeMode);
			//fieldDelimiter check
			this.fieldDelimiter = this.writerSliceConfig.getString(Key.FIELD_DELIMITER,null);
			if(null == fieldDelimiter){
				throw DataXException.asDataXException(HiveWriterErrorCode.REQUIRED_VALUE,
						String.format("您提供配置文件有误，[%s]是必填参数.", Key.FIELD_DELIMITER));
			}else if(1 != fieldDelimiter.length()){
				// warn: if have, length must be one
				throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE,
						String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", fieldDelimiter));
			}
			//compress check
			this.compress  = this.writerSliceConfig.getString(Key.COMPRESS,null);
			if(fileType.equalsIgnoreCase("TEXT")){
				Set<String> textSupportedCompress = Sets.newHashSet("GZIP", "BZIP2");
				//用户可能配置的是compress:"",空字符串,需要将compress设置为null
				if(StringUtils.isBlank(compress) ){
					this.writerSliceConfig.set(Key.COMPRESS, null);
				}else {
					compress = compress.toUpperCase().trim();
					if(!textSupportedCompress.contains(compress) ){
						throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE,
								String.format("目前TEXT FILE仅支持GZIP、BZIP2 两种压缩, 不支持您配置的 compress 模式 : [%s]",
										compress));
					}
				}
			}else if(fileType.equalsIgnoreCase("ORC")){
				Set<String> orcSupportedCompress = Sets.newHashSet("NONE", "SNAPPY");
				if(null == compress){
					this.writerSliceConfig.set(Key.COMPRESS, "NONE");
				}else {
					compress = compress.toUpperCase().trim();
					if(!orcSupportedCompress.contains(compress)){
						throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE,
								String.format("目前ORC FILE仅支持SNAPPY压缩, 不支持您配置的 compress 模式 : [%s]",
										compress));
					}
				}
			}else if (fileType.equalsIgnoreCase("PARQUET")){
				Set<String> parquetSupportedCompress = Sets.newHashSet("GZIP", "SNAPPY");
				if(null == compress){
					this.writerSliceConfig.set(Key.COMPRESS, "NONE");
				}else {
					compress = compress.toUpperCase().trim();
					if(!parquetSupportedCompress.contains(compress)){
						throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE,
								String.format("目前PARQUET FILE仅支持GZIP、SNAPPY 种压缩, 不支持您配置的 compress 模式 : [%s]",
										compress));
					}
				}

			}
			//Kerberos check
			Boolean haveKerberos = this.writerSliceConfig.getBool(Key.HAVE_KERBEROS, false);
			if(haveKerberos) {
				this.writerSliceConfig.getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, HiveWriterErrorCode.REQUIRED_VALUE);
				this.writerSliceConfig.getNecessaryValue(Key.KERBEROS_PRINCIPAL, HiveWriterErrorCode.REQUIRED_VALUE);
			}
			// encoding check
			this.encoding = this.writerSliceConfig.getString(Key.ENCODING,Constant.DEFAULT_ENCODING);
			try {
				encoding = encoding.trim();
				this.writerSliceConfig.set(Key.ENCODING, encoding);
				Charsets.toCharset(encoding);
			} catch (Exception e) {
				throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE,
						String.format("不支持您配置的编码格式:[%s]", encoding), e);
			}
		}

		@Override
		public void prepare() {
			// hdfs操作，数据原始上传hdfs路径
			if(hdfsHelper.isPathexists(path)){
//				hdfsHelper.hdfsChangeOwner(path, hdfsUsername, hdfsGroupname);
//				hdfsHelper.hdfsChangeMode(path, ugoMode);
				hdfsHelper.hdfsCurrentParentPathPermession(defaultFS, path, hdfsUsername, hdfsGroupname, ugoMode);
				hdfsHelper.hdfsRecursionPermession(defaultFS, path, hdfsUsername, hdfsGroupname, ugoMode);
				// 判断不是合法目录
				if(!hdfsHelper.isPathDir(path)){
					throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE,
							String.format("您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
									path));
				}
				//根据writeMode对目录下文件进行处理
				Path[] existFilePaths = hdfsHelper.hdfsDirList(path,fileName);
				boolean isExistFile = false;
				if(existFilePaths.length > 0){
					isExistFile = true;
				}
				/**
				 if ("truncate".equals(writeMode) && isExistFile ) {
				 LOG.info(String.format("由于您配置了writeMode truncate, 开始清理 [%s] 下面以 [%s] 开头的内容",
				 path, fileName));
				 hdfsHelper.deleteFiles(existFilePaths);
				 } else
				 */
				if ("append".equalsIgnoreCase(writeMode)) {
					LOG.info(String.format("由于您配置了writeMode append, 写入hdfs前不做清理工作, hdfs目录: [%s] 下面写入相应文件名前缀  [%s] 的文件",
							path, fileName));
				} else if ("nonconflict".equalsIgnoreCase(writeMode) && isExistFile) {
					LOG.info(String.format("由于您配置了writeMode nonConflict, 开始检查hdfs目录: [%s] 下面的内容", path));
					List<String> allFiles = new ArrayList<String>();
					for (Path eachFile : existFilePaths) {
						allFiles.add(eachFile.toString());
					}
					LOG.error(String.format("冲突文件列表为: [%s]", StringUtils.join(allFiles, ",")));
					throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE,
							String.format("由于您配置了writeMode nonConflict,但您配置hdfs目录的path: [%s] 目录不为空, 下面存在其他文件或文件夹.", path));
				}else if ("truncate".equalsIgnoreCase(writeMode) && isExistFile) {
					LOG.info(String.format("由于您配置了writeMode truncate, hdfs目录: [%s] 下面的内容将被覆盖重写", path));
					hdfsHelper.deleteFiles(existFilePaths);
				}
			}else{
//                throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE, String.format("您配置的path: [%s] 不存在, 请先在hive端创建对应的数据库和表.", path));
				LOG.info(String.format("您配置的hdfs目录: [%s] 不存在, 自动创建此路径", path));
				hdfsHelper.createPath(path, hdfsUsername, hdfsGroupname, ugoMode);
//				hdfsHelper.hdfsChangeOwner(path, hdfsUsername, hdfsGroupname);
//				hdfsHelper.hdfsChangeMode(path, ugoMode);
				hdfsHelper.hdfsCurrentParentPathPermession(defaultFS, path, hdfsUsername, hdfsGroupname, ugoMode);
				hdfsHelper.hdfsRecursionPermession(defaultFS, path, hdfsUsername, hdfsGroupname, ugoMode);

			}

			//检查hive表是否存在，hive location是否存在
			boolean isHiveTableExists = hdfsHelper.isHiveTableExists(url, userName, password, dbName, tableName);
			if (isHiveTableExists) {
//				LOG.info("hiveTableLocation = " + hdfsHelper.getHiveTableLocation(url, userName, password, dbName, tableName));

				// hive操作, 数据load到hive表路径
				if(hdfsHelper.isPathexists(locationPath)) {
					// 判断不是合法目录
					if(!hdfsHelper.isPathDir(locationPath)){
						throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE,
								String.format("您配置的locationPath: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
										locationPath));
					}
//					hdfsHelper.hdfsChangeOwner(locationPath, hdfsUsername, hdfsGroupname);
//					hdfsHelper.hdfsChangeMode(locationPath, ugoMode);
					hdfsHelper.hdfsCurrentParentPathPermession(defaultFS, locationPath, hdfsUsername, hdfsGroupname, ugoMode);
					hdfsHelper.hdfsRecursionPermession(defaultFS, locationPath, hdfsUsername, hdfsGroupname, ugoMode);

					if (tableType.equals("normal")) {
						String hiveNorLocationPath = locationPath;
						//根据writeMode对目录下文件进行处理
						Path[] existHiveNorFilePaths = hdfsHelper.hdfsDirList(hiveNorLocationPath, fileName);
						boolean isExistHiveNorFile = false;
						if(existHiveNorFilePaths.length > 0){
							isExistHiveNorFile = true;
						}
						//根据writeMode对目录下文件进行处理
						if ("append".equalsIgnoreCase(writeMode)) {
							LOG.info(String.format("由于您配置了Hive表的writeMode append, Hive表写入前不做清理工作, [%s] 目录下写入相应文件名前缀  [%s] 的文件",
									hiveNorLocationPath, fileName));
						} else if ("nonconflict".equalsIgnoreCase(writeMode) && isExistHiveNorFile) {
							LOG.info(String.format("由于您配置了Hive表的writeMode nonConflict, 开始检查 [%s] 下面的内容", hiveNorLocationPath));
							List<String> allFiles = new ArrayList<String>();
							for (Path eachFile : existHiveNorFilePaths) {
								allFiles.add(eachFile.toString());
							}
							LOG.error(String.format("Hive表数据冲突文件列表为: [%s]", StringUtils.join(allFiles, ",")));
							throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE,
									String.format("由于您配置了Hive表的writeMode nonConflict,但您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.", path));
						} else if ("truncate".equalsIgnoreCase(writeMode) && isExistHiveNorFile) {
							LOG.info(String.format("由于您配置了Hive表的writeMode truncate,  [%s] 下面的内容将被覆盖重写", hiveNorLocationPath));
							hdfsHelper.deleteFiles(existHiveNorFilePaths);
						}

					}
					//分区表处理
					else if (tableType.equals("partition")) {
						String hiveParLocationPath = "";
						ArrayList<String> hiveParLocationPathArr = new ArrayList<>();
						StringBuilder hiveParLocationPathSBuilder = new  StringBuilder();
						hiveParLocationPathArr.add(locationPath);
						if (parColumns.size() > 0) {
							for (Configuration eachParColumn : parColumns) {
								String eachParColumnName = eachParColumn.getNecessaryValue(Key.NAME, HiveWriterErrorCode.COLUMN_REQUIRED_VALUE);
								String eachParColumnType = eachParColumn.getNecessaryValue(Key.TYPE, HiveWriterErrorCode.COLUMN_REQUIRED_VALUE);
								String eachParColumnValue = eachParColumn.getNecessaryValue(Key.VALUE, HiveWriterErrorCode.COLUMN_REQUIRED_VALUE);
								hiveParLocationPathArr.add(eachParColumnName + "=" + eachParColumnValue);
							}
							for (String s : hiveParLocationPathArr) {
								hiveParLocationPathSBuilder.append(s);
								hiveParLocationPathSBuilder.append("/");
							}
						} else {
							throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE,
									String.format("您配置了Hive分区表，但是分区表的信息错误；[%s] 不是一个合法的分区配置, 请您注意配置分区信息等情况.",
											parColumns));
						}
						hiveParLocationPath = hiveParLocationPathSBuilder.toString();
						//分区数据处理
						if (hdfsHelper.isPathexists(hiveParLocationPath)) {
							// 已存在分区
							Path[] existHiveParFilePaths = hdfsHelper.hdfsDirList(hiveParLocationPath, fileName);
							boolean isExistHiveParFile = false;
							if(existHiveParFilePaths.length > 0){
								isExistHiveParFile = true;
							}
							if ("append".equalsIgnoreCase(writeMode)) {
								LOG.info(String.format("由于您配置了Hive表的writeMode append, Hive表写入前不做清理工作, [%s] 目录下写入相应文件名前缀  [%s] 的文件",
										hiveParLocationPath, fileName));
							} else if ("nonconflict".equalsIgnoreCase(writeMode) && isExistHiveParFile) {
								LOG.info(String.format("由于您配置了Hive表的writeMode nonConflict, 开始检查 [%s] 下面的内容", hiveParLocationPath));
								List<String> allFiles = new ArrayList<String>();
								for (Path eachFile : existHiveParFilePaths) {
									allFiles.add(eachFile.toString());
								}
								LOG.error(String.format("Hive表数据冲突文件列表为: [%s]", StringUtils.join(allFiles, ",")));
								throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE,
										String.format("由于您配置了Hive表的writeMode nonConflict,但您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.", path));
							} else if ("truncate".equalsIgnoreCase(writeMode) && isExistHiveParFile) {
								LOG.info(String.format("由于您配置了Hive表的writeMode truncate,  [%s] 下面的内容将被覆盖重写", hiveParLocationPath));
								hdfsHelper.deleteFiles(existHiveParFilePaths);
							}
						}else {
							// 不存在分区
							LOG.info(String.format("由于您配置了Hive表的分区: [%s] 不存在, 数据将自动加载到新的分区。", hiveParLocationPath));
						}
					}
				}else {
					// 如果hive表存在，但是hive表location目录不存在
					throw DataXException.asDataXException(HiveWriterErrorCode.ILLEGAL_VALUE,
							String.format("您配置的Hive表: [%s] 元数据存在, 但是location目录: [%s] 不存在, 请您注意检查Hive配置目录等情况.",
									tableName, locationPath));
				}
			}else {
				// 如果Hive表不存在，则自动创建
				hdfsHelper.createPath(locationPath, hdfsUsername, hdfsGroupname, ugoMode);
//				hdfsHelper.hdfsChangeOwner(locationPath, hdfsUsername, hdfsGroupname);
//				hdfsHelper.hdfsChangeMode(locationPath, ugoMode);
				hdfsHelper.hdfsCurrentParentPathPermession(defaultFS, locationPath, hdfsUsername, hdfsGroupname, ugoMode);
				hdfsHelper.hdfsRecursionPermession(defaultFS, locationPath, hdfsUsername, hdfsGroupname, ugoMode);

				hdfsHelper.createHiveTable(locationPath, url, userName, password, tableType, dbName, tableName, columns, parColumns);
			}
		}

		@Override
		public void post() {
			hdfsHelper.renameFile(tmpFiles, endFiles);


//			hdfsHelper.hdfsChangeOwner(path, hdfsUsername, hdfsGroupname);
//			hdfsHelper.hdfsChangeMode(path, ugoMode);
			hdfsHelper.hdfsCurrentParentPathPermession(defaultFS, path, hdfsUsername, hdfsGroupname, ugoMode);

			hdfsHelper.hdfsRecursionPermession(defaultFS, path, hdfsUsername, hdfsGroupname,ugoMode);
//			hdfsHelper.hdfsChangeOwner(locationPath,hdfsUsername,hdfsGroupname);
//			hdfsHelper.hdfsChangeMode(locationPath, ugoMode);
			hdfsHelper.hdfsCurrentParentPathPermession(defaultFS, locationPath, hdfsUsername, hdfsGroupname, ugoMode);

			hdfsHelper.hdfsRecursionPermession(defaultFS, locationPath, hdfsUsername, hdfsGroupname,ugoMode);

			hdfsHelper.loadData2Hive(path, url, userName, password, tableType, dbName, tableName, parColumns);
		}

		@Override
		public void destroy() {
			hdfsHelper.closeFileSystem();
		}

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			LOG.info("begin do split...");
			List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
			String filePrefix = fileName;

			Set<String> allFiles = new HashSet<String>();

			//获取该路径下的所有已有文件列表
			if(hdfsHelper.isPathexists(path)){
				allFiles.addAll(Arrays.asList(hdfsHelper.hdfsDirList(path)));
			}

			String fileSuffix;
			//临时存放路径
			String storePath =  buildTmpFilePath(this.path);
			//最终存放路径
			String endStorePath = buildFilePath();
			this.path = endStorePath;
			for (int i = 0; i < mandatoryNumber; i++) {
				// handle same file name

				Configuration splitedTaskConfig = this.writerSliceConfig.clone();
				String fullFileName = null;
				String endFullFileName = null;

				fileSuffix = UUID.randomUUID().toString().replace('-', '_');

				fullFileName = String.format("%s%s%s__%s", defaultFS, storePath, filePrefix, fileSuffix);
				endFullFileName = String.format("%s%s%s__%s", defaultFS, endStorePath, filePrefix, fileSuffix);

				while (allFiles.contains(endFullFileName)) {
					fileSuffix = UUID.randomUUID().toString().replace('-', '_');
					fullFileName = String.format("%s%s%s__%s", defaultFS, storePath, filePrefix, fileSuffix);
					endFullFileName = String.format("%s%s%s__%s", defaultFS, endStorePath, filePrefix, fileSuffix);
				}
				allFiles.add(endFullFileName);

				//设置临时文件全路径和最终文件全路径
				if("GZIP".equalsIgnoreCase(this.compress)){
					this.tmpFiles.add(fullFileName + ".gz");
					this.endFiles.add(endFullFileName + ".gz");
				}else if("BZIP2".equalsIgnoreCase(compress)){
					this.tmpFiles.add(fullFileName + ".bz2");
					this.endFiles.add(endFullFileName + ".bz2");
				}else{
					this.tmpFiles.add(fullFileName);
					this.endFiles.add(endFullFileName);
				}

				splitedTaskConfig
						.set(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
								fullFileName);

				LOG.info(String.format("splited write file name:[%s]",
						fullFileName));

				writerSplitConfigs.add(splitedTaskConfig);
			}
			LOG.info("end do split.");
			return writerSplitConfigs;
		}

		private String buildFilePath() {
			boolean isEndWithSeparator = false;
			switch (IOUtils.DIR_SEPARATOR) {
				case IOUtils.DIR_SEPARATOR_UNIX:
					isEndWithSeparator = this.path.endsWith(String
							.valueOf(IOUtils.DIR_SEPARATOR));
					break;
				case IOUtils.DIR_SEPARATOR_WINDOWS:
					isEndWithSeparator = this.path.endsWith(String
							.valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
					break;
				default:
					break;
			}
			if (!isEndWithSeparator) {
				this.path = this.path + IOUtils.DIR_SEPARATOR;
			}
			return this.path;
		}

		/**
		 * 创建临时目录
		 * @param userPath
		 * @return
		 */
		private String buildTmpFilePath(String userPath) {
			String tmpFilePath;
			boolean isEndWithSeparator = false;
			switch (IOUtils.DIR_SEPARATOR) {
				case IOUtils.DIR_SEPARATOR_UNIX:
					isEndWithSeparator = userPath.endsWith(String
							.valueOf(IOUtils.DIR_SEPARATOR));
					break;
				case IOUtils.DIR_SEPARATOR_WINDOWS:
					isEndWithSeparator = userPath.endsWith(String
							.valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
					break;
				default:
					break;
			}
			String tmpSuffix;
			tmpSuffix = UUID.randomUUID().toString().replace('-', '_');
			if (!isEndWithSeparator) {
				tmpFilePath = String.format("%s__%s%s", userPath, tmpSuffix, IOUtils.DIR_SEPARATOR);
			}else if("/".equals(userPath)){
				tmpFilePath = String.format("%s__%s%s", userPath, tmpSuffix, IOUtils.DIR_SEPARATOR);
			}else{
				tmpFilePath = String.format("%s__%s%s", userPath.substring(0,userPath.length()-1), tmpSuffix, IOUtils.DIR_SEPARATOR);
			}
			while(hdfsHelper.isPathexists(tmpFilePath)){
				tmpSuffix = UUID.randomUUID().toString().replace('-', '_');
				if (!isEndWithSeparator) {
					tmpFilePath = String.format("%s__%s%s", userPath, tmpSuffix, IOUtils.DIR_SEPARATOR);
				}else if("/".equals(userPath)){
					tmpFilePath = String.format("%s__%s%s", userPath, tmpSuffix, IOUtils.DIR_SEPARATOR);
				}else{
					tmpFilePath = String.format("%s__%s%s", userPath.substring(0,userPath.length()-1), tmpSuffix, IOUtils.DIR_SEPARATOR);
				}
			}
			return tmpFilePath;
		}

		public void unitizeParquetConfig(Configuration writerSliceConfig) {
			String parquetSchema = writerSliceConfig.getString(Key.PARQUET_SCHEMA);
			if (StringUtils.isNotBlank(parquetSchema)) {
				LOG.info("parquetSchema has config. use parquetSchema:\n{}", parquetSchema);
				return;
			}

			List<Configuration> columns = writerSliceConfig.getListConfiguration(Key.COLUMN);
			if (columns == null || columns.isEmpty()) {
				throw DataXException.asDataXException("parquetSchema or column can't be blank!");
			}

			parquetSchema = generateParquetSchemaFromColumn(columns);
			// 为了兼容历史逻辑,对之前的逻辑做保留，但是如果配置的时候报错，则走新逻辑
			try {
				MessageTypeParser.parseMessageType(parquetSchema);
			} catch (Throwable e) {
				LOG.warn("The generated parquetSchema {} is illegal, try to generate parquetSchema in another way", parquetSchema);
				parquetSchema = HiveHelper.generateParquetSchemaFromColumnAndType(columns);
				LOG.info("The last generated parquet schema is {}", parquetSchema);
			}
			writerSliceConfig.set(Key.PARQUET_SCHEMA, parquetSchema);
			LOG.info("dataxParquetMode use default fields.");
			writerSliceConfig.set(Key.DATAX_PARQUET_MODE, "fields");
		}

		private String generateParquetSchemaFromColumn(List<Configuration> columns) {
			StringBuffer parquetSchemaStringBuffer = new StringBuffer();
			parquetSchemaStringBuffer.append("message m {");
			for (Configuration column: columns) {
				String name = column.getString("name");
				Validate.notNull(name, "column.name can't be null");

				String type = column.getString("type");
				Validate.notNull(type, "column.type can't be null");

				String parquetColumn = String.format("optional %s %s;", type, name);
				parquetSchemaStringBuffer.append(parquetColumn);
			}
			parquetSchemaStringBuffer.append("}");
			String parquetSchema = parquetSchemaStringBuffer.toString();
			LOG.info("generate parquetSchema:\n{}", parquetSchema);
			return parquetSchema;
		}

	}

	public static class Task extends Writer.Task {
		private static final Logger LOG = LoggerFactory.getLogger(HiveWriter.Task.class);

		private Configuration writerSliceConfig;

		private String url;
		private String userName;
		private String password;
		private String dbName;
		private String tableName;
		private String parColumn;
		private String parName;

		private String defaultFS;
		private String fileType;
		private String fileName;
		private String path;

		private HiveHelper hdfsHelper = null;

		@Override
		public void init() {
			this.writerSliceConfig = this.getPluginJobConf();

			this.defaultFS = this.writerSliceConfig.getString(Key.DEFAULT_FS);
			this.fileType = this.writerSliceConfig.getString(Key.FILE_TYPE);
			//得当的已经是绝对路径，eg：hdfs://10.101.204.12:9000/user/hive/warehouse/writer.db/text/test.textfile
			this.fileName = this.writerSliceConfig.getString(Key.FILE_NAME);
			this.path = this.writerSliceConfig.getString(Key.PATH);

//			this.url = this.writerSliceConfig.getString(Key.URL);
//			this.userName = this.writerSliceConfig.getString(Key.USERANAME);
//			this.password = this.writerSliceConfig.getString(Key.PASSWORD);
//
//			this.dbName = this.writerSliceConfig.getString(Key.DB_NAME);
//			this.tableName = this.writerSliceConfig.getString(Key.TABLE_NAME);
//			this.parColumn = this.writerSliceConfig.getString(Key.PAR_COLUMN);

			hdfsHelper = new HiveHelper();
			hdfsHelper.getFileSystem(defaultFS, writerSliceConfig);
		}

		@Override
		public void prepare() {

		}

		@Override
		public void startWrite(RecordReceiver lineReceiver) {
			LOG.info("begin do write...");
			LOG.info(String.format("write to file : [%s]", this.fileName));
			if(fileType.equalsIgnoreCase("TEXT")){
				//写TEXT FILE
				hdfsHelper.textFileStartWrite(lineReceiver,this.writerSliceConfig, this.fileName,
						this.getTaskPluginCollector());
			}else if(fileType.equalsIgnoreCase("ORC")){
				//写ORC FILE
				hdfsHelper.orcFileStartWrite(lineReceiver,this.writerSliceConfig, this.fileName,
						this.getTaskPluginCollector());
			}else if (fileType.equalsIgnoreCase("PARQUET")){
				//写PARQUET FILE
				hdfsHelper.parquetFileStartWrite(lineReceiver,this.writerSliceConfig, this.fileName,
						this.getTaskPluginCollector());
			}

			LOG.info("end do write");

		}

		@Override
		public void post() {

		}

		@Override
		public void destroy() {

		}
	}
}
