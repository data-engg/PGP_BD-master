/*
 * This code contains API to interact with HBase. It can create table, put values, get values, delete values, delete column & column family and drop table.
 * Deprecate APIs have been used only for printing results. Else, all deprecate APIs are avoided.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;

public class HbaseCurdInteractive {
	
	static Configuration conf=null;						//creates a configuration object
	
	static {
		conf = HBaseConfiguration.create(); 			//adds Hbase Configuration files to Configuration
		conf.clear();
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
	}
	
	public static boolean createTable(String tableName, ArrayList<String> colFamily) throws IOException {
		/*
		 * ConnectionFactorycreateConnection returns an object of connection class. Connection.getAdmin returns an object of Admin class.
		 * HTableDescriptor holds the description of table. 
		 */
		Admin hadmin = ConnectionFactory.createConnection(conf).getAdmin();
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
		
		if (hadmin.tableExists(TableName.valueOf(tableName))) {		//checks if table exists
			System.out.println(tableName + " already exists");
			hadmin.close();
			return false;
		} else {
			for (String col : colFamily) {
				table.addFamily(new HColumnDescriptor(col));		//adds a column family to table descriptor
			}
			hadmin.createTable(table);								//create table with column families added
			hadmin.close();
			return true;
		}		
	}
	
	public static boolean putTable(String tableName, String record) throws IOException {
		String[] records = record.trim().split(",");
		Admin hadmin = ConnectionFactory.createConnection(conf).getAdmin();
		Table table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
		
		if (hadmin.tableExists(TableName.valueOf(tableName))) {
			Put put = new Put(Bytes.toBytes(records[0].trim())); 	//instantiate Put with rowid
			
			put.addColumn(Bytes.toBytes(records[1].trim()),		 
					Bytes.toBytes(records[2].trim()), 
					Bytes.toBytes(records[3].trim()));				//adds column family, qualifier, value to put object 
			table.put(put);
			hadmin.close();
			return true;
		} else {
			System.out.println(tableName + " table does not exists");
			hadmin.close();
			return false;
		}
		
	}
	
	@SuppressWarnings( "deprecation")
	public static boolean getTable(String tableName, String row) throws IOException{
		
		Admin hadmin = ConnectionFactory.createConnection(conf).getAdmin();
		Table table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));	
		
		if (hadmin.tableExists(TableName.valueOf(tableName))) {
			Get get = new Get(Bytes.toBytes(row));			//instantiate Get object with rowid
			Result res = table.get(get);					//get method on table return an object of Result class
			for (KeyValue kv : res.raw()) {
				System.out.print(new String(kv.getRow()) + " ");
				System.out.print(new String(kv.getFamily()) + " ");
				System.out.print(new String(kv.getQualifier()) + " ");
				System.out.print(kv.getTimestamp() + " ");
				System.out.print(new String(kv.getValue()) + " ");
				System.out.println();
			}
			hadmin.close();
			return true;
		} else {
			System.out.println(tableName + " table does not exists");
			hadmin.close();
			return false;
		}		
	}
	
	@SuppressWarnings( "deprecation")
	public static boolean getTable(String tableName, String row, String colfam) throws IOException{
		Admin hadmin = ConnectionFactory.createConnection(conf).getAdmin();
		Table table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
		
		if (hadmin.tableExists(TableName.valueOf(tableName))) {
			Get get = new Get(Bytes.toBytes(row));			//instantiate Get object with rowid
			get.addFamily(Bytes.toBytes(colfam));			//add column family to get object
			Result res = table.get(get);
			for (KeyValue kv : res.raw()) {
				System.out.print(new String(kv.getRow()) + " ");
				System.out.print(new String(kv.getFamily()) + " ");
				System.out.print(new String(kv.getQualifier()) + " ");
				System.out.print(kv.getTimestamp() + " ");
				System.out.print(new String(kv.getValue()) + " ");
				System.out.println();
			}
			hadmin.close();
			return true;
		} else {
			System.out.println(tableName + " table does not exists");
			hadmin.close();
			return false;
		}		
	}
	
	@SuppressWarnings( "deprecation")
	public static boolean getTable(String tableName, String row, String colfam, String qualifier) throws IOException{
		Admin hadmin = ConnectionFactory.createConnection(conf).getAdmin();
		Table table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
		
		if (hadmin.tableExists(TableName.valueOf(tableName))) {
			Get get = new Get(Bytes.toBytes(row));							//instantiate Get object with rowid
			get.addColumn(Bytes.toBytes(colfam), Bytes.toBytes(qualifier)); //add column family and qualifier to get object
			Result res = table.get(get);
			for (KeyValue kv : res.raw()) {
				System.out.print(new String(kv.getRow()) + " ");
				System.out.print(new String(kv.getFamily()) + " ");
				System.out.print(new String(kv.getQualifier()) + " ");
				System.out.print(kv.getTimestamp() + " ");
				System.out.print(new String(kv.getValue()) + " ");
				System.out.println();
			}
			hadmin.close();
			return true;
		} else {
			System.out.println(tableName + " table does not exists");
			hadmin.close();
			return false;
		}		
	}
	
	public static boolean deleteTable(String tableName) throws IOException{
		Admin hadmin = ConnectionFactory.createConnection(conf).getAdmin();
		TableName tablename = TableName.valueOf(tableName);
		
		if (hadmin.tableExists(tablename)) {
			if (hadmin.isTableDisabled(tablename)) {	//checks if table is disable?
				hadmin.deleteTable(tablename);			//deleteTable method on Admin onbject deletes table
				return true;
			} else {
				hadmin.disableTable(tablename);
				hadmin.deleteTable(tablename);
				return true;
			}
		} else {
			System.out.println(tableName + " does not exist");
			return false;
		}
	}
	
	public static boolean deleteValue(String tableName, String record) throws IOException{
		String[] records = record.trim().split(",");
		Admin hadmin = ConnectionFactory.createConnection(conf).getAdmin();
		Table table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
		
		if (hadmin.tableExists(TableName.valueOf(tableName))) {
			Delete del = new Delete(Bytes.toBytes(records[0]));				//Delete object is instantiated with rowid
			del.addColumn(Bytes.toBytes(records[1]), 
					Bytes.toBytes(records[2]));								//column family and qualifier is added to delete object
			table.delete(del);												//delete method of table deletes columns
			hadmin.close();
			return true;
		} else {
			System.out.println(tableName + " table does not exists");
			hadmin.close();
			return false;
		}
	}
	
	public static boolean deleteCF(String tableName, String colfal) throws IOException {
		Admin hadmin = ConnectionFactory.createConnection(conf).getAdmin();
				
		if (hadmin.tableExists(TableName.valueOf(tableName))) {
			hadmin.deleteColumn(TableName.valueOf(tableName), 
					Bytes.toBytes(colfal));						//deleteColumn method of Admin deletes column family 
			return true;
		} else {
			System.out.println(tableName + " table does not exists");
			return false;
		}
		
		
	}
	
	@SuppressWarnings( "deprecation")
	public static void printData(String tableName) throws IOException {
		
		Admin hadmin = ConnectionFactory.createConnection(conf).getAdmin();
		Table table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
		
		if (hadmin.tableExists(TableName.valueOf(tableName))) {
			Scan scan = new Scan();								//scanner is instantiated
			ResultScanner rs = table.getScanner(scan);			//getScanner of table returns ResultScanner
			
			for (Result result : rs) {							//ResultScanner is an array of Result objects
				for (KeyValue kv : result.raw()) {				//raw method on Result return object of KeyValue class which can be used to print values
					System.out.print(new String(kv.getRow()) + " ");
					System.out.print(new String(kv.getFamily()) + " ");
					System.out.print(new String(kv.getQualifier()) + " ");
					System.out.print(kv.getTimestamp() + " ");
					System.out.print(new String(kv.getValue()) + " ");
					System.out.println();
				}
			}
		} else {
			System.out.println(tableName + " table does not exists");
		}
	}
	
	public static void main(String[] args) throws IOException {
		Scanner inputScanner = new Scanner(System.in);
		String tableName=null;
		System.out.println("+------------------------------+");
		System.out.println("| Welcome to Hbase API utility |");
		System.out.println("+------------------------------+");
		
		while (true) {
			
			System.out.println("Choose the option");
			System.out.println("1. Create table");
			System.out.println("2. Insert into table");
			System.out.println("3. Get row value");
			System.out.println("4. Delete table");
			System.out.println("5. Delete value");
			System.out.println("6. Delete column family");
			System.out.println("7. Print table data");
			System.out.println("8. Exit");
			Integer option = Integer.parseInt(inputScanner.nextLine());
			
			switch (option) {
			case 1 : {
				//logic to create table
				ArrayList<String> colfams = new ArrayList<String>();
				
				System.out.println("enter the table name to be created");
				tableName = inputScanner.nextLine();
				System.out.println("enter the number of column families");
				int colcount = Integer.parseInt(inputScanner.nextLine().trim());
				
				if (colcount > 0) {
					
					for (int i=0;i<colcount;i++) {
						System.out.print("Enter col family : " + (i+1) + " : ");
						colfams.add(inputScanner.nextLine());
					}
					System.out.println(HbaseCurdInteractive.createTable(tableName, colfams));
				}
				break;
				
			} case 2 : {
				//logic to insert in table
				
				System.out.println("enter the table name where data is to inserted");
				tableName = inputScanner.nextLine();
				System.out.println("Insert record in the format \"RowKey,ColumnFamily,ColumnQualifier,Value\"");
				String record=inputScanner.nextLine();
				
				if (record.split(",").length == 4) {
					System.out.println(HbaseCurdInteractive.putTable(tableName, record));
				} else {
					System.out.println("enter values in correct format");
				}
				break;
				
			}case 3 :{
				//logic to get result
				
				System.out.println("enter the table from where row is to be fetched");
				tableName = inputScanner.nextLine();
				System.out.println("Enter rowid, column family (optional), qualifier (optional) in format \"rowid,colfal,qual\"");
				String[] record = inputScanner.nextLine().trim().split(",");
				if (record.length <= 3 && record.length > 0) {
					if(record.length == 1) {
						System.out.println(HbaseCurdInteractive.getTable(tableName, record[0]));
					} else if (record.length == 2) {
						System.out.println(HbaseCurdInteractive.getTable(tableName, record[0], record[1]));
					} else if (record.length == 3){
						System.out.println(HbaseCurdInteractive.getTable(tableName, record[0], record[1], record[2]));
					}
				} else {
					System.out.println("enter values in correct format");
				}
				break;
			}
			
			case 4 : {
				//logic to delete table
				
				System.out.println("enter the table which will is to be deleted");
				tableName = inputScanner.nextLine();
				System.out.println(HbaseCurdInteractive.deleteTable(tableName));
				break;
				
			} case 5 : {
				//logic to delete value
				
				System.out.println("enter the table name where data is to be deleted");
				tableName = inputScanner.nextLine();
				System.out.println("Inset value to be deleted in format \"RowKey,ColumnFamily,ColumnQualifier\"");
				String record = inputScanner.nextLine();
				
				if (record.split(",").length == 3) {
					System.out.println(HbaseCurdInteractive.deleteValue(tableName, record.trim()));
				} else {
					System.out.println("enter values in correct format");
				}
				break;
				
			} case 6 : {
				//logic to delete column family
				
				System.out.println("enter the table name");
				tableName = inputScanner.nextLine();
				System.out.println("enter the column family");
				String colfal = inputScanner.nextLine();
				System.out.println(HbaseCurdInteractive.deleteCF(tableName, colfal));
				break;
				
			} case 7 : {
				//logic to print table
				
				System.out.println("enter the table name");
				tableName = inputScanner.nextLine();
				HbaseCurdInteractive.printData(tableName);
				break;
				
			} case 8 : {
				inputScanner.close();
				System.out.println("Exiting.....");
				System.exit(0);
			} default :
				System.out.println("Select valid option");
				break;
			}
		}	
	}
}
