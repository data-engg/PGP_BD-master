// This is simple twitter producer. It reads the tweets and displays in stdout

import twitter4j.TwitterStream;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterStreamFactory;
import twitter4j.RawStreamListener;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterException;

public class TwitterProducerMain {

    public static void main(String... args) {
        TwitterStream twitterStream = createTwitterStream();
        twitterStream.addListener(createListener());
        twitterStream.sample();
    }

    private static TwitterStream createTwitterStream() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("...")
                .setOAuthConsumerSecret("...")
                .setOAuthAccessToken("...")
                .setOAuthAccessTokenSecret("...");

        return new TwitterStreamFactory(cb.build()).getInstance();
    }

    private static RawStreamListener createListener() {
        return new TweetsStatusListener();
    }


    public static class TweetsStatusListener implements RawStreamListener{

        @Override
        public void onMessage(String tweetJson) {
            try{
                Status status = TwitterObjectFactory.createStatus(tweetJson);
                if (status.getUser() != null){
                    System.out.println(tweetJson);
                    System.out.println(status.getUser());
                    System.out.println(status.getText());
                }

            } catch (TwitterException e){
                e.printStackTrace();
            }
        }

        @Override
        public void onException(Exception e) {

        }
    }
}
