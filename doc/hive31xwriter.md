# DataX Hive31xWriter 插件文档


------------

## 1 快速介绍

Hive31xWriter提供向Hive数据库系统指定路径中写入TEXTFile文件和ORCFile文件,自动加载HDFS数据到hive中的表。


## 2 功能与限制

* (1)、目前Hive31xWriter仅支持textfile和orcfile两种格式的文件，且文件内容存放的必须是一张逻辑意义上的二维表;
* (2)、由于HDFS是文件系统，不存在schema的概念，因此不支持对部分列写入;
* (3)、目前仅支持与以下Hive数据类型：
数值型：TINYINT,SMALLINT,INT,BIGINT,FLOAT,DOUBLE
字符串类型：STRING,VARCHAR,CHAR
布尔类型：BOOLEAN
时间类型：DATE,TIMESTAMP
**目前不支持：decimal、binary、arrays、maps、structs、union类型**;
* (4)、对于Hive分区表目前仅支持一次写入单个分区;
* (5)、对于textfile需用户保证写入hdfs文件的分隔符**与在Hive上创建表时的分隔符一致**,从而实现写入hdfs数据与Hive表字段关联;
* (6)、Hive31xWriter实现过程是：首先根据用户指定的path，创建一个hdfs文件系统上不存在的临时目录，创建规则：path_随机；然后将读取的文件写入这个临时目录；全部写入后再将这个临时目录下的文件移动到用户指定目录（在创建文件时保证文件名不重复）; 最后删除临时目录。如果在中间过程发生网络中断等情况造成无法与hdfs建立连接，需要用户手动删除已经写入的文件和临时目录。
* (7)、目前插件中Hive版本为3.1.0，Hadoop版本为3.1.1（Apache［为适配JDK1.8］,在Hadoop 3.1.1, Hadoop 3.2.0 和Hive 3.1.0测试环境中写入正常；其它版本需后期进一步测试；
* (8)、目前Hive31xWriter支持Kerberos认证（注意：如果用户需要进行kerberos认证，那么用户使用的Hadoop集群版本需要和hdfsreader的Hadoop版本保持一致，如果高于hdfsreader的Hadoop版本，不保证kerberos认证有效）
* (9)、Hive31xWriter自动创建指定HDFS目录，并根据配置设置目录所属用户，所有组，以及权限。
* (10)、Hive31xWriter自动将配置文件中指定HDFS路径的数据加载到配置文件中指定的表，或者指定表的分区。
* (11)、Hive31xWriter自动根据配置文件中的列信息，和分区信息，location信息，表类型信息等，创建Hive普通表或分区表。
* (12)、Hive31xWriter自动加载指定HDFS路径的数据到Hive表，支持自动多级分区。
## 3 功能说明


### 3.1 配置样例

```json
{
	"job": {
		"setting": {
			"speed": {
				"channel": 1
			}
		},
		"content": [
			{
				"reader": {
					"parameter": {
						"column": [
							"id",
							"spu_id",
							"price",
							"sku_name",
							"sku_desc",
							"weight",
							"tm_id",
							"category3_id",
							"sku_default_img",
							"is_sale",
							"create_time"
						],
						"connection": [
							{
								"table": [
									"sku_info"
								],
								"jdbcUrl": [
									"jdbc:mysql://192.168.0.23:5505/mall?useUnicode=true&characterEncoding=UTF-8"
								]
							}
						],
						"username": "root",
						"password": "123456",
						"splitPk": ""
					},
					"name": "mysqlreader"
				},
				"writer": {
					"name": "hive31xwriter",
					"parameter": {
						"column": [
							{
								"type": "bigint",
								"name": "id"
							},
							{
								"type": "bigint",
								"name": "spu_id"
							},
							{
								"type": "string",
								"name": "price"
							},
							{
								"type": "string",
								"name": "sku_name"
							},
							{
								"type": "string",
								"name": "sku_desc"
							},
							{
								"type": "string",
								"name": "weight"
							},
							{
								"type": "bigint",
								"name": "tm_id"
							},
							{
								"type": "bigint",
								"name": "category3_id"
							},
							{
								"type": "string",
								"name": "sku_default_img"
							},
							{
								"type": "bigint",
								"name": "is_sale"
							},
							{
								"type": "string",
								"name": "create_time"
							}
						],
						"writeMode": "append",
						"fieldDelimiter": "\t",
						"compress": "gzip",
						"defaultFS": "hdfs://192.168.0.45:8020",
						"fileType": "text",
						"path": "/datasource/dev/test/sku_info/2022-10-13",
						"fileName": "sku_info",
						"hdfsUsername": "hive",
						"hdfsGroupname": "hadoop",
						"url": "jdbc:hive2://192.168.0.45:10000/dev",
						//hive表的location
						"locationPath": "/dev/test/sku_info",
						"userName": "hive",
						"password": "hive",
						"dbName": "dev",
						"tableName": "sku_info",
						//hdfs目录和文件的权限，可以设置为all/a:777,read/r:644,write/w:622,execute/x:611
						//read_write/rw:666,read_execute/rx:655,write_execute/wx:633,none:600
						"ugoMode": "all",
						"superUser": "hdfs",
						"superGroup": "hadoop",
						// tableType只支持partition和normal
						"tableType": "partition",
						//当tableType选择partition时，parColumn为必选项，支持多级分区。
						//当tableType选择normal时，parColumn为可选项，设置与否不起作用。
						"parColumn": [
							{
								"name": "year",
								"type": "string",
								"value": "2022"
							},
							{
								"name": "month",
								"type": "string",
								"value": "10"
							}
						]

					}
				}
			}
		]
	}
}
```

### 3.2 参数说明

* **defaultFS**

    * 描述：Hadoop hdfs文件系统namenode节点地址。格式：hdfs://ip:端口；例如：hdfs://127.0.0.1:8020<br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **fileType**

    * 描述：文件的类型，目前只支持用户配置为"text"或"orc"。 <br />

        text表示textfile文件格式

        orc表示orcfile文件格式

    * 必选：是 <br />

    * 默认值：无 <br />
* **path**

    * 描述：存储到Hadoop hdfs文件系统的路径信息，Hive31xWriter会根据并发配置在Path目录下写入多个文件。为与hive表关联，请填写hive表在hdfs上的存储路径。例：Hive上设置的数据仓库的存储路径为：/user/hive/warehouse/ ，已建立数据库：test，表：hello；则对应的存储路径为：/user/hive/warehouse/test.db/hello  <br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **fileName**

     * 描述：Hive31xWriter写入时的文件名，实际执行时会在该文件名后添加随机的后缀作为每个线程写入实际文件名。 <br />

    * 必选：是 <br />

    * 默认值：无 <br />
* **column**

    * 描述：写入数据的字段，不支持对部分列写入。为与hive中表关联，需要指定表中所有字段名和字段类型，其中：name指定字段名，type指定字段类型。 <br />

        用户可以指定Column字段信息，配置如下：

        ```json
        "column":
                 [
                            {
                                "name": "userName",
                                "type": "string"
                            },
                            {
                                "name": "age",
                                "type": "long"
                            }
                 ]
        ```

    * 必选：是 <br />

    * 默认值：无 <br />
* **writeMode**

     * 描述：Hive31xWriter写入前数据清理处理模式： <br />

        * append，写入前不做任何处理，DataX Hive31xWriter直接使用filename写入，并保证文件名不冲突。
        * nonConflict，如果目录下有fileName前缀的文件，直接报错。
        * truncate，如果目录下有fileName前缀的文件，先删除后写入。

    * 必选：是 <br />

    * 默认值：无 <br />

* **fieldDelimiter**

    * 描述：Hive31xWriter写入时的字段分隔符,**需要用户保证与创建的Hive表的字段分隔符一致，否则无法在Hive表中查到数据** <br />

    * 必选：是 <br />

    * 默认值：无 <br />

* **compress**

    * 描述：hdfs文件压缩类型，默认不填写意味着没有压缩。其中：text类型文件支持压缩类型有gzip、bzip2;orc类型文件支持的压缩类型有NONE、SNAPPY（需要用户安装SnappyCodec）。 <br />

    * 必选：否 <br />

    * 默认值：无压缩 <br />

* **hadoopConfig**

    * 描述：hadoopConfig里可以配置与Hadoop相关的一些高级参数，比如HA的配置。<br />

        ```json
        "hadoopConfig":{
                "dfs.nameservices": "testDfs",
                "dfs.ha.namenodes.testDfs": "namenode1,namenode2",
                "dfs.namenode.rpc-address.aliDfs.namenode1": "",
                "dfs.namenode.rpc-address.aliDfs.namenode2": "",
                "dfs.client.failover.proxy.provider.testDfs": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        }
        ```

    * 必选：否 <br />

     * 默认值：无 <br />

* **encoding**

    * 描述：写文件的编码配置。<br />

     * 必选：否 <br />

     * 默认值：utf-8，**慎重修改** <br />

* **haveKerberos**

    * 描述：是否有Kerberos认证，默认false<br />
 
         例如如果用户配置true，则配置项kerberosKeytabFilePath，kerberosPrincipal为必填。

     * 必选：haveKerberos 为true必选 <br />
 
     * 默认值：false <br />

* **kerberosKeytabFilePath**

    * 描述：Kerberos认证 keytab文件路径，绝对路径<br />

     * 必选：否 <br />
 
     * 默认值：无 <br />

* **kerberosPrincipal**

    * 描述：Kerberos认证Principal名，如xxxx/hadoopclient@xxx.xxx <br />

     * 必选：haveKerberos 为true必选 <br />
 
     * 默认值：无 <br />

* **hdfsUsername**
    * 描述
     * 必须：是 <br />
     * 默认值：无 <br />

* **hdfsGroupname**
    * 描述
     * 必须：是 <br />
     * 默认值：无 <br />

* **superUser**
    * 描述
    * 必须：是 <br />
    * 默认值：hdfs <br />

* **superGroup**
	* 描述
	* 必须：否 <br />
	* 默认值：hadoop <br />
	
* **url**
	* 描述
	* 必须：是 <br />
	* 默认值：无 <br />

* **locationPath**
	* 描述
	* 必须：是 <br />
	* 默认值：无 <br />

* **userName**
	* 描述
	* 必须：是 <br />
	* 默认值：无 <br />

* **password**
	* 描述
	* 必须：否 <br />
	* 默认值：无 <br />

* **dbName**
	* 描述
	* 必须：是 <br />
	* 默认值：无 <br />

* **tableName**
	* 描述
	* 必须：是 <br />
	* 默认值：无 <br />

* **tableType**
	* 描述：写入数据的Hive表的类型，目前只支持用户配置为"normal"或"partition"。
	* 必须：是 <br />
	* 默认值：无 <br />
	
* **ugoMode**
	* 描述：写入数据的HDFS目录或文件的权限，例如：all/rw/rx/wx等。
	* 必须：是 <br />
	* 默认值：无 <br />

* **parColumn**
	* 描述：写入数据的Hive表的分区字段，支持多级分区。为与hive中表关联，需要指定表中所有分区字段名、字段类型、分区值，其中：name指定字段名，type指定字段类型，value指定分区值。 <br />

	  用户可以指定parColumn字段信息，仅支持单值分区，不支持范围分区，配置如下：

	    ```json
        "parColumn":
                 [
                            {
                                "name": "year",
                                "type": "string",
                                "value": "2022",
   
                            },
                            {
                                "name": "month",
                                "type": "string",
                                "value": "11",
   
                            }
                 ]
        ```

	* 必须：tableType 为 partition必选 <br />
	* 默认值：无 <br />













### 3.3 类型转换

目前 Hive31xWriter 支持大部分 Hive 类型，请注意检查你的类型。

下面列出 Hive31xWriter 针对 Hive 数据类型转换列表:

| DataX 内部类型| HIVE 数据类型    |
| -------- | -----  |
| Long     |TINYINT,SMALLINT,INT,BIGINT |
| Double   |FLOAT,DOUBLE |
| String   |STRING,VARCHAR,CHAR |
| Boolean  |BOOLEAN |
| Date     |DATE,TIMESTAMP |


## 4 配置步骤
* 步骤一、在Hive中创建数据库、表
Hive数据库在HDFS上存储配置,在hive安装目录下 conf/hive-site.xml文件中配置，默认值为：/user/hive/warehouse
如下所示：

```xml
<property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
    <description>location of default database for the warehouse</description>
  </property>
```
Hive建库／建表语法 参考 [Hive操作手册]( https://cwiki.apache.org/confluence/display/Hive/LanguageManual)

例：
（1）建立存储为textfile文件类型的表
```json
create database IF NOT EXISTS Hive31xWriter;
use Hive31xWriter;
create table text_table(
col1  TINYINT,
col2  SMALLINT,
col3  INT,
col4  BIGINT,
col5  FLOAT,
col6  DOUBLE,
col7  STRING,
col8  VARCHAR(10),
col9  CHAR(10),
col10  BOOLEAN,
col11 date,
col12 TIMESTAMP
)
row format delimited
fields terminated by "\t"
STORED AS TEXTFILE;
```
text_table在hdfs上存储路径为：/user/hive/warehouse/Hive31xWriter.db/text_table/

（2）建立存储为orcfile文件类型的表
```json
create database IF NOT EXISTS Hive31xWriter;
use Hive31xWriter;
create table orc_table(
col1  TINYINT,
col2  SMALLINT,
col3  INT,
col4  BIGINT,
col5  FLOAT,
col6  DOUBLE,
col7  STRING,
col8  VARCHAR(10),
col9  CHAR(10),
col10  BOOLEAN,
col11 date,
col12 TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC;
```
orc_table在hdfs上存储路径为：/user/hive/warehouse/Hive31xWriter.db/orc_table/

* 步骤二、根据步骤一的配置信息配置Hive31xWriter作业

## 5 约束限制

略

## 6 FAQ

略
