//This a simple application to read data flat file and write it kinesis streams synchronously

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.amazonaws.services.kinesis.producer.Attempt;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.Future;

public class SynchronousProduce {

    public static void main(String[] args)  {
        File file = new File(args[0]);
        writeRecords(file);

    }

    //method is create and configure KinesisProducer
    private static KinesisProducer createProducer() {
        KinesisProducerConfiguration conf = new KinesisProducerConfiguration();
        conf.setRequestTimeout(6000);
        conf.setRecordMaxBufferedTime(15000);
        conf.setRegion("ap-south-1");

        return new KinesisProducer(conf);
    }

    
    private static void writeRecords(File file){
        Scanner sc = null;
        int count = 0;
        
        //List will be used to collect Future objects returned after writing data to stream
        List<Future<UserRecordResult>> putFutures = new LinkedList<Future<UserRecordResult>>();

        try{
            sc = new Scanner(file);
            KinesisProducer kinesisProducer = createProducer();
            // Read file and save records in Futures
            while (sc.hasNext()){

                ByteBuffer data = ByteBuffer.wrap(sc.nextLine().getBytes(StandardCharsets.UTF_8));
                 
                //writing to kinesis stream
                //Future objects are returned from addUserRecord can be used to check results
                putFutures.add(
                        kinesisProducer.addUserRecord(
                           "myStream"
                            ,"0"
                            ,data
                        ));
                count += 1;
            }

            // Wait for puts to finish and check the results
            for (Future<UserRecordResult> f : putFutures){
                UserRecordResult result = f.get();

                if (result.isSuccessful()){
                    System.out.println("Records put in shard " + result.getShardId());
                } else {
                    for (Attempt attempt : result.getAttempts()){
                        System.out.println( attempt.getErrorMessage());
                    }
                }
            }


        } catch (FileNotFoundException fe){
            System.out.println( file.getName() + " : does not exists");
        } catch (Exception e){

        }
    }

}
