/*Usage guidelines 
 * E.g If empty files havebe removed from directory /example then the argument should be supplied as below:
 * 
 * hadoop jar RemoveEmptyFilesUtility.jar RemoveEmptyFilesUtility /example
*/

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;

public class RemoveEmptyFilesUtility {

	public static void main(String[] args) {
		
		if (args.length != 1){
			System.out.println("Expeted one directory from where deletion has to be made as arguement");
			System.out.println("E.g. if empty files from /example direcotry has to be removed then arguements should be supplied as :");
			System.out.println("hadoop jar RemoveEmptyFilesUtility.jar RemoveEmptyFilesUtility /example");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost/");
		
		try{
			
			FileSystem fs = FileSystem.get(conf);			
			RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(new Path(args[0]), true);
			
			while (fileIterator.hasNext()){
				LocatedFileStatus tempStatus = fileIterator.next();
				if (tempStatus.getLen() == 0)
					if (fs.delete(tempStatus.getPath(), false))
						System.out.println(tempStatus.getPath() +" : deleted");			
			}
			
		} catch (IOException e){
			System.out.println("File system does not exists");
		}
	}

}
