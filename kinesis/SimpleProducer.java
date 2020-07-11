//This a simple application to read data flat file and write it kinesis streams

import java.io.File;
import java.util.Scanner;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.KinesisProducer;


public class SimpleProducer {


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
        try{
            sc = new Scanner(file);
            while (sc.hasNext()){
                KinesisProducer kinesisProducer = createProducer();
                
                //writing to kinesis stream
                kinesisProducer.addUserRecord("myStream",                   //stream name 
                        "0",                                                //partition key
                        ByteBuffer.wrap(sc.nextLine().getBytes(StandardCharsets.UTF_8)));       //data is wrapped in a byte array
                count += 1;
            }
            System.out.println(count + " : lines written to stream");
        } catch (FileNotFoundException fe){
            System.out.println( file.getName() + " : does not exists");
        } catch (Exception e){

        }
    }


}
