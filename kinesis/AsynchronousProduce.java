//This is an application to read data from file and write to a stream asynchronously
import com.amazonaws.services.kinesis.producer.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class AsynchronousProduce {

    public static void main(String[] args) throws UnsupportedEncodingException {
        File file = new File(args[0]);
        writeRecords(file);

    }

    private static KinesisProducer createProducer() {
        KinesisProducerConfiguration conf = new KinesisProducerConfiguration();
        conf.setRequestTimeout(6000);
        conf.setRecordMaxBufferedTime(15000);
        conf.setRegion("ap-south-1");

        return new KinesisProducer(conf);
    }

    private static void writeRecords(File file) {

        KinesisProducer kinesis = createProducer();
        Scanner sc = null;
        int count = 0;

        try{
            sc = new Scanner(file);
            while (sc.hasNext()){

                ByteBuffer data = ByteBuffer.wrap(sc.nextLine().getBytes(StandardCharsets.UTF_8));               
                
                // If the Future is complete by the time we call addCallback, the callback will be invoked immediately.
                
                Futures.addCallback(
                        kinesis.addUserRecord("myStream","0",data)
                        , new FutureCallback<UserRecordResult>() {
                            @Override
                            public void onSuccess(UserRecordResult userRecordResult) {
                                System.out.println(userRecordResult.getShardId());
                            }

                            @Override
                            public void onFailure(Throwable throwable) {
                                throwable.printStackTrace();
                            }
                        });
                count += 1;

            }
            System.out.println(count + " : lines written to stream");
        } catch (FileNotFoundException fe){
            System.out.println( file.getName() + " : does not exists");
        } catch (Exception e){

        }
    }
}
