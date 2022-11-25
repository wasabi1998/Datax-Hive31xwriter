package com.alibaba.datax.plugin.writer.hive31xwriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @ProjectName： DataX-datax_v202210-modifybyxiaom
 * @packageName: com.alibaba.datax.plugin.writer.hive31xwriter
 * @className: testListFiles
 * @authorName: xiaom
 * @createDate: 2022/11/24 9:07
 * @version: 1.0
 * @description:
 */
public class testListFiles {

	public static final String HDFS_PATH = "hdfs://192.168.0.45:8020";
	FileSystem fileSystem = null;
	Configuration configuration = null;
	Path path = new Path("/dev/test/sku_info2");

	@Before
	public void setUp() throws Exception{
		System.out.println("setUp-----------");
		configuration = new Configuration();
		configuration.set("dfs.replication","1");
		/**
		 * 构造一个访问制定HDFS系统的客户端对象
		 * 第一个参数：HDFS的URI
		 * 第二个参数：客户端制定的配置参数
		 * 第三个参数：客户端的身份，说白了就是用户名
		 */
		fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hive");
	}

	@Test
	public void getSubPath(Path path) {
		try {
			FileStatus[] fileStatuses = fileSystem.listStatus(path);
			if (fileStatuses != null && fileStatuses.length > 0) {
				for (FileStatus fileStatus : fileStatuses) {
					if (fileStatus.isDirectory()) {
						Path subPath = fileStatus.getPath();
						System.out.println("========================" + subPath);
						Path recursionPath = new Path(subPath.toString());
						getSubPath(recursionPath);

					}
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void getPath() throws IOException {
//		BufferedWriter writer = new BufferedWriter(new FileWriter("D:\\skutestPath1.log"));

		ArrayList<String> pathArr = new ArrayList<>();
		try {
			if (fileSystem != null && path != null) {
				//获取文件列表
				FileStatus[] files = fileSystem.listStatus(path);
				Path[] paths = FileUtil.stat2Paths(files);
				for (Path p : paths) {
//				writer.write(p.toString());
//				writer.newLine();
					pathArr.add(p.toString());
					pathArr.add("aaa");
				}
				System.out.println("###" + pathArr.toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ;
	}

	public void getPath2() throws IOException {

//		BufferedWriter writer = new BufferedWriter(new FileWriter("D:\\skutestPath2.log"));

		HashMap<String, ArrayList> recursionPathHashMap = new HashMap<>();

		System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		try {
//            Pattern pattern = Pattern.compile(".map$");
			if (fileSystem != null && path != null) {
				//获取文件列表
				FileStatus[] files = fileSystem.listStatus(path);
				//展示文件信息
				for (int i = 0; i < files.length; i++) {
					if (files[i].isDirectory()) {
						//生成Key
						String recursionPathHashMapKey = files[i].getPath().toString();
						recursionPathHashMap.put(files[i].getPath().toString(), new ArrayList());
						System.out.println(">>>" + files[i].getPath()+ ", dir owner:" + files[i].getOwner());
//					writer.write(files[i].getPath().toString());
//					writer.newLine();
						//递归调用获取子目录生成value
//					getPath(files[i].getPath());
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void testListStatus() throws Exception{
		String hdfsPath = "/dev/test/sku_info2";
		FileStatus[] statuses = fileSystem.listStatus(new Path(hdfsPath));
		for(FileStatus file : statuses){
			String isDir = file.isDirectory() ? "文件夹" : "文件";
			String permission = file.getPermission().toString();
			short replication = file.getReplication();
			long length = file.getLen();
			String path = file.getPath().toString();
			System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + length + "\t" + path);
		}
	}

	public testListFiles() throws IOException {
	}

	@After
	public void tearDown(){
		configuration = null;
		fileSystem = null;
		System.out.println("----------tearDown------");
	}
}
