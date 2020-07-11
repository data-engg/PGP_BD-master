/* This is a utility to sync files between two directories in hdfs. It is configured to run in local mode. 
  *To run it in cluster mode edit line 38 as:
  *conf.set("fs.defaultFS", <namespace_name> ) E.g. if namespace is name_space1 then the code is edited as:
  *conf.set("fs.defaultFS", "hdfs://name_space1");

*Usage guidelines 
 * E.g If hdfs directory /source and /target needs to be synced then the argument should be supplied as below:
 * 
 *		java HdfsSynUtility /source /target
 */

import java.util.ArrayList;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;

public class HdfsSynUtility {
	public static void main(String[] args) throws Exception{
		
		if (args.length != 2){
			System.out.println("Expeted two imput parameters");
			System.out.println("E.g. if hdfs directory /source and /target needs to be synced then the argument should be supplied as below");
			System.out.println("java HdfsSynUtility /source /target");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost/" ); //Setting configuration
		
		FileSystem fs = FileSystem.get(conf);
		Path src_hdfs = new Path(args[0]);
		Path tgt_hdfs = new Path(args[1]);
		ArrayList<String> src_files = new ArrayList<String>();
		ArrayList<String> tgt_files = new ArrayList<String>();
		 
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(src_hdfs, false);
		
		while (fileStatusListIterator.hasNext()){
			src_files.add(fileStatusListIterator.next().getPath().toString());		//Iterating through source hdfs destination to get source files
			}
		
		fileStatusListIterator = fs.listFiles(tgt_hdfs, false);
		
		while (fileStatusListIterator.hasNext()){
			tgt_files.add(fileStatusListIterator.next().getPath().toString());		//Iterating through target hdfs destination to get soure files
			}
		
		for (String file : src_files){
			String src_file_name = file.replace("hdfs://localhost".concat(src_hdfs.toString()), "");
			
			if (tgt_files.contains(file.replace("source", "target"))){				//checking if the file in source is present in target
				
				if (fs.getFileChecksum(new Path(file)).equals(fs.getFileChecksum(new Path(file.replace("source", "target")))) ){	//checking if the data in files having same name is the same or not
					
					System.out.println(file + " : " + file.replace("source", "target").toString() + " are same");
					
				} else {					
					
					if (HDFSJavaAPI.HdfsDelete(tgt_hdfs.toString().concat(src_file_name), false, conf)){
						if (HDFSJavaAPI.HdfsToHdfs(file, tgt_hdfs.toString().concat(src_file_name), conf)){
							System.out.println(tgt_hdfs.toString().concat(src_file_name) + " updated");			//updating target file with data in source file
						} else {
							System.out.println(tgt_hdfs.toString().concat(src_file_name) + " could not be updated");
						}
					} else {
						System.out.println("file could not be deleted");
					}
				}
					
			} else {
				
				if (HDFSJavaAPI.HdfsToHdfs(file, tgt_hdfs.toString().concat(src_file_name), conf)){
					System.out.println(file + " : copied to " + tgt_hdfs.toString().concat(src_file_name));		//copying file from source to target
				} else {
					System.out.println(file + " : could not be copied to " + tgt_hdfs.toString().concat(src_file_name));
				}
			}
		}		
	}
}

class HDFSJavaAPI {
	
	public static boolean HdfsDelete(String hdfsPath, boolean recursive, Configuration conf) throws Exception{
		
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(hdfsPath);
		
		if (fs.exists(path)){
			fs.delete(path, recursive);
			return true;
		} else {
			System.out.println("HDFS path : " +hdfsPath+ " does not exists" );
			return false;
		}
	}

	public static boolean localToHdfs(String source, String destination, Configuration conf) throws Exception{
	
	FileSystem fs = FileSystem.get(conf);
	Path path = new Path(destination);
	
	if (fs.exists(path)){
		System.out.println("File system exists : "+path);
		return false;
	} 
	
	FSDataOutputStream fsout = fs.create(path);
	InputStream fin = new BufferedInputStream(new FileInputStream(new File(source)));
	
	byte[] b = new byte[1024];
	int numbytes;
	
	while ((numbytes = fin.read(b)) != -1){
		fsout.write(b, 0, numbytes);
	}
	
	fsout.close();
	fin.close();
	
	return true;
	}
	
	public static boolean HdfsTOLocal(String source, String destination, Configuration conf) throws Exception{
		
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(source);
		
		if (!fs.exists(path)){
			System.out.println("HDFS file does not exist");
			return false;
		}
		
		FSDataInputStream fsin = fs.open(path);
		OutputStream fout = new BufferedOutputStream(new FileOutputStream(new File(destination)));
		
		byte[] b = new byte[1024];
		int numBytes = 0;
		
		while ((numBytes = fsin.read(b)) > 0)
			fout.write(b, 0, numBytes);
		
		fsin.close();
		fout.close();
		
		return true;
	}

	public static boolean HdfsToHdfs(String source, String destination, Configuration conf) throws Exception {
		
		FileSystem fs = FileSystem.get(conf);
		
		Path src_path = new Path(source);
		Path tgt_path = new Path(destination);
		
		if ( ! fs.exists(src_path)){
			System.out.println("Source file system does not exist");
			return false;
		}
		
		if ( fs.exists(tgt_path) ){
			System.out.println("Target file system already exists");
			return false;
		}
		
		FSDataInputStream fsin = fs.open(src_path);
		FSDataOutputStream fsout = fs.create(tgt_path);
		
		IOUtils.copyBytes(fsin, fsout, 1024, false);

		
		return true;
	}
}
