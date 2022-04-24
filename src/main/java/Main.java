import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.text.Normalizer;
import java.util.*;
import java.util.stream.Stream;


public class Main {
    /**
     * Create class and schema for Tweet class
     * Schema:
     *      tweet_id (long):       tweet ID
     *      create_at (datetime):  tweet create time
     *      text (string):         text of tweet
     *      user_id (long):        ID of user who post this tweet
     *      geo (string):          location of where this tweet being post
     *      retweet_id (long):     original tweet ID of the retweeted tweet
     *      reply_id (long):       original tweet ID of the replied tweet
     *      quote_id (long):       original tweet ID of the quoted tweet
     *
     * Note that retweet_id, reply_id, quote_id might be NULL depends on the situations.
     * @param db OrientDB TwitterDB connection
     */
    private static void createTweetSchema(ODatabaseSession db) {
        OClass tweet = db.getClass("Tweet");
        if (tweet == null) {
            tweet = db.createVertexClass("Tweet");
        }
        if (tweet.getProperty("tweet_id") == null) {
            tweet.createProperty("tweet_id", OType.LONG);
            tweet.createIndex("Tweet_id_index", OClass.INDEX_TYPE.NOTUNIQUE, "tweet_id");
        }
        if (tweet.getProperty("create_at") == null) {
            tweet.createProperty("create_at", OType.DATETIME);
            tweet.createIndex("Create_at_index", OClass.INDEX_TYPE.NOTUNIQUE, "create_at");
        }
        if (tweet.getProperty("text") == null) {
            tweet.createProperty("text", OType.STRING);
            tweet.createIndex("Text_index", OClass.INDEX_TYPE.NOTUNIQUE, "text");
        }
        if (tweet.getProperty("user_id") == null) {
            tweet.createProperty("user_id", OType.LONG);
            tweet.createIndex("Tweet_user_id_index", OClass.INDEX_TYPE.NOTUNIQUE, "user_id");
        }
        if (tweet.getProperty("geo") == null) {
            tweet.createProperty("geo", OType.STRING);
            tweet.createIndex("Geo_index", OClass.INDEX_TYPE.NOTUNIQUE, "geo");
        }
        if (tweet.getProperty("retweet_id") == null) {
            tweet.createProperty("retweet_id", OType.LONG);
            tweet.createIndex("Retweet_id_index", OClass.INDEX_TYPE.NOTUNIQUE, "retweet_id");
        }
        if (tweet.getProperty("reply_id") == null) {
            tweet.createProperty("reply_id", OType.LONG);
            tweet.createIndex("Reply_id_index", OClass.INDEX_TYPE.NOTUNIQUE, "reply_id");
        }
        if (tweet.getProperty("quote_id") == null) {
            tweet.createProperty("quote_id", OType.LONG);
            tweet.createIndex("Quote_id_index", OClass.INDEX_TYPE.NOTUNIQUE, "quote_id");
        }
//        if (tweet.getProperty("hashtag") == null) {
//            tweet.createProperty("hashtag", OType.EMBEDDEDLIST);
//            tweet.createIndex("Hashtag_index", OClass.INDEX_TYPE.NOTUNIQUE, "hashtag");
//        }
    }

    /**
     * Create class and schema for Tweet class
     * Schema:
     *      user_id (long):         user ID
     *      screen_name (string):  user's username
     *      country (string):      user's country
     *      verified (boolean):    whether this user is verified or not
     *
     * Note that country might be NULL as users have the right to set it to private or never provided
     * @param db OrientDB TwitterDB connection
     */
    private static void createUserSchema(ODatabaseSession db) {
        OClass user = db.getClass("User");
        if (user == null) {
            user = db.createVertexClass("User");
        }
        if (user.getProperty("user_id") == null) {
            user.createProperty("user_id", OType.LONG);
            user.createIndex("User_id_index", OClass.INDEX_TYPE.NOTUNIQUE, "user_id");
        }
        if (user.getProperty("screen_name") == null) {
            user.createProperty("screen_name", OType.STRING);
            user.createIndex("Screen_name_index", OClass.INDEX_TYPE.NOTUNIQUE, "screen_name");
        }
        if (user.getProperty("country") == null) {
            user.createProperty("country", OType.STRING);
            user.createIndex("Country_index", OClass.INDEX_TYPE.NOTUNIQUE, "country");
        }
        if (user.getProperty("verified") == null) {
            user.createProperty("verified", OType.BOOLEAN);
            user.createIndex("Verified_index", OClass.INDEX_TYPE.NOTUNIQUE, "verified");
        }
    }

    /**
     * Create class and schema for Hashtag class
     * Schema:
     *      hashtag (String):       hashtag name
     * @param db OrientDB TwitterDB connection
     */
    private static void createHashtagSchema(ODatabaseSession db) {
        OClass hashtag = db.getClass("Hashtag");
        if (hashtag == null) {
            hashtag = db.createVertexClass("Hashtag");
        }
        if (hashtag.getProperty("hashtag") == null) {
            hashtag.createProperty("hashtag", OType.STRING);
            hashtag.createIndex("Hashtag_index", OClass.INDEX_TYPE.NOTUNIQUE, "hashtag");
        }
    }

    /**
     * Create Tweet class object from input properties
     * @param db OrientDB TwitterDB connection
     * @param tweetId tweet ID
     * @param datetime tweet creation time
     * @param text original text of this tweet
     * @param userId ID of user who post this tweet
     * @param geo location where user post this tweet
     * @param retweetId original tweet ID of the retweeted tweet
     * @param replyId original tweet ID of the replied tweet
     * @param quoteId original tweet ID of the quoted tweet
     * @return created Tweet vertex in database
     * NOTE: After calling this function, the vertex is already been added to the database.
     *       The return OVertex is for modifying and usage of add edges
     */
    private static OVertex createTweetVertex(ODatabaseSession db, long tweetId, Date datetime, String text, long userId, String geo, long retweetId, long replyId, long quoteId) {
        OVertex tweet = db.newVertex("Tweet");
        tweet.setProperty("tweet_id", tweetId);
        tweet.setProperty("create_at", datetime);
        tweet.setProperty("text", text);
        tweet.setProperty("user_id", userId);
        tweet.setProperty("geo", geo);
        tweet.setProperty("retweet_id", retweetId);
        tweet.setProperty("reply_id", replyId);
        tweet.setProperty("quote_id", quoteId);
        tweet.save();
//        System.out.println("Created Tweet Vertex with tweet_id = " + tweetId);
        return tweet;
    }

    /**
     * Create User class object from input properties
     * @param db OrientDB TwitterDB connection
     * @param userId user ID
     * @param screenName user's username
     * @param country user's country
     * @param verified whether this user is verified or not
     * @return created User vertex in database
     * NOTE: Similar to Tweet vertex, it will be automatically added to the database
     */
    private static OVertex createUserVertex(ODatabaseSession db, long userId, String screenName, String country, boolean verified) {
        OVertex user = db.newVertex("User");
        user.setProperty("user_id", userId);
        user.setProperty("screen_name", screenName);
        user.setProperty("country", country);
        user.setProperty("verified", verified);
        user.save();
//        System.out.println("Created User Vertex with user_id = " + userId);
        return user;
    }

    /**
     * Create Hashtag class object from input properties
     * @param db OrientDB TwitterDB connection
     * @param hashtag hashtag name
     * @return created Hashtag vertex in database
     * NOTE: Similar to Tweet vertex, it will be automatically added to the database
     */
    private static OVertex createHashtagVertex(ODatabaseSession db, String hashtag) {
        OVertex vertex = db.newVertex("Hashtag");
        vertex.setProperty("hashtag", hashtag);
        vertex.save();
//        System.out.println("Created Hashtag Vertex with hashtag = " + hashtag);
        return vertex;
    }

    /**
     * Create relation classes:
     *      post:     User  -> Tweet
     *      retweet:  Tweet -> Tweet
     *      reply:    Tweet -> Tweet
     *      quote:    Tweet -> Tweet
     *      contain:  Tweet -> Hashtag
     * @param db OrientDB TwitterDB connection
     */
    private static void createRelations(ODatabaseSession db) {
        if (db.getClass("post") == null) {
            db.createEdgeClass("post");
        }
        if (db.getClass("retweet") == null) {
            db.createEdgeClass("retweet");
        }
        if (db.getClass("reply") == null) {
            db.createEdgeClass("reply");
        }
        if (db.getClass("quote") == null) {
            db.createEdgeClass("quote");
        }
        if (db.getClass("contain") == null) {
            db.createEdgeClass("contain");
        }
    }

    /**
     * Create "post" relation edge class object for given user_id and tweet_id
     * @param db OrientDB TwitterDB connection
     * @param userId given user ID
     * @param tweetId given tweet ID
     */
    private static void createPostEdge(ODatabaseSession db, long userId, long tweetId) {
        String query1 = String.format("SELECT * FROM User WHERE user_id = %d", userId);
        OResultSet iter1 = db.query(query1);
        Stream<OVertex> userList = iter1.vertexStream();
        Optional<OVertex> optionUser = userList.findFirst();

        String query2 = String.format("SELECT * FROM Tweet WHERE tweet_id = %d", tweetId);
        OResultSet iter2 = db.query(query2);
        Stream<OVertex> tweetList = iter2.vertexStream();
        Optional<OVertex> optionTweet = tweetList.findFirst();

        if (optionUser.isPresent() && optionTweet.isPresent()) {
//            System.out.println("create post edge");
            OVertex user = optionUser.get();
            OVertex tweet = optionTweet.get();
            OEdge edge = db.newEdge(user, tweet, "post");
            edge.save();
        }
    }

    /**
     * Create "retweet" relation edge class object for given tweet_id and retweet_id
     * @param db OrientDB TwitterDB connection
     * @param tweetId given tweet ID
     * @param retweetId given retweet ID
     */
    private static void createRetweetEdge(ODatabaseSession db, long tweetId, long retweetId) {
        String query1 = String.format("SELECT * FROM Tweet WHERE tweet_id = %d", tweetId);
        OResultSet iter1 = db.query(query1);
        Stream<OVertex> tweetList = iter1.vertexStream();
        Optional<OVertex> optionTweet = tweetList.findFirst();

        String query2 = String.format("SELECT * FROM Tweet WHERE tweet_id = %d", retweetId);
        OResultSet iter2 = db.query(query2);
        Stream<OVertex> retweetList = iter2.vertexStream();
        Optional<OVertex> optionRetweet = retweetList.findFirst();

        if (optionTweet.isPresent() && optionRetweet.isPresent()) {
            OVertex tweet = optionTweet.get();
            OVertex retweet = optionRetweet.get();
            OEdge edge = db.newEdge(tweet, retweet, "retweet");
            edge.save();
        }
    }

    /**
     * Create "reply" relation edge class object for given tweet_id and reply_id
     * @param db OrientDB TwitterDB connection
     * @param tweetId given tweet ID
     * @param replyId given replied tweet ID
     */
    private static void createReplyEdge(ODatabaseSession db, long tweetId, long replyId) {
        String query1 = String.format("SELECT * FROM Tweet WHERE tweet_id = %d", tweetId);
        OResultSet iter1 = db.query(query1);
        Stream<OVertex> tweetList = iter1.vertexStream();
        Optional<OVertex> optionTweet = tweetList.findFirst();

        String query2 = String.format("SELECT * FROM Tweet WHERE tweet_id = %d", replyId);
        OResultSet iter2 = db.query(query2);
        Stream<OVertex> replyList = iter2.vertexStream();
        Optional<OVertex> optionReply = replyList.findFirst();

        if (optionTweet.isPresent() && optionReply.isPresent()) {
            OVertex tweet = optionTweet.get();
            OVertex reply = optionReply.get();
            OEdge edge = db.newEdge(tweet, reply, "reply");
            edge.save();
        }
    }

    /**
     * Create "quote" relation edge class object for given tweet_id and quote_id
     * @param db OrientDB TwitterDB connection
     * @param tweetId given tweet ID
     * @param quoteId given quoted tweet ID
     */
    private static void createQuoteEdge(ODatabaseSession db, long tweetId, long quoteId) {
        String query1 = String.format("SELECT * FROM Tweet WHERE tweet_id = %d", tweetId);
        OResultSet iter1 = db.query(query1);
        Stream<OVertex> tweetList = iter1.vertexStream();
        Optional<OVertex> optionTweet = tweetList.findFirst();

        String query2 = String.format("SELECT * FROM Tweet WHERE tweet_id = %d", quoteId);
        OResultSet iter2 = db.query(query2);
        Stream<OVertex> quoteList = iter2.vertexStream();
        Optional<OVertex> optionQuote = quoteList.findFirst();

        if (optionTweet.isPresent() && optionQuote.isPresent()) {
            OVertex tweet = optionTweet.get();
            OVertex quote = optionQuote.get();
            OEdge edge = db.newEdge(tweet, quote, "quote");
            edge.save();
        }
    }

    /**
     * Create "contain" relation edge class object for given tweet_id and hashtag
     * @param db OrientDB TwitterDB connection
     * @param tweetId given tweet ID
     * @param hashtag given hashtag name
     */
    private static void createContainEdge(ODatabaseSession db, long tweetId, String hashtag) {
        String query1 = String.format("SELECT * FROM Tweet WHERE tweet_id = %d", tweetId);
        OResultSet iter1 = db.query(query1);
        Stream<OVertex> tweetList = iter1.vertexStream();
        Optional<OVertex> optionTweet = tweetList.findFirst();

        String query2 = String.format("SELECT * FROM Hashtag WHERE hashtag = \"%s\"", hashtag);
        OResultSet iter2 = db.query(query2);
        Stream<OVertex> hashtagList = iter2.vertexStream();
        Optional<OVertex> optionHashtag = hashtagList.findFirst();

        if (optionTweet.isPresent() && optionHashtag.isPresent()) {
            OVertex tweet = optionTweet.get();
            OVertex hashtagRes = optionHashtag.get();
            OEdge edge = db.newEdge(tweet, hashtagRes, "contain");
            edge.save();
        }
    }

    /**
     * For given tweet_id, check whether it is already in the database
     * @param db OrientDB TwitterDB connection
     * @param tweetId given tweet_id
     * @return true for Tweet in database, false o.w.
     */
    public static boolean checkTweetVertex(ODatabaseSession db, long tweetId) {
        String query1 = String.format("SELECT * FROM Tweet WHERE tweet_id = %d", tweetId);
        OResultSet iter1 = db.query(query1);
        Stream<OVertex> tweetList = iter1.vertexStream();
        Optional<OVertex> optionTweet = tweetList.findFirst();
        return optionTweet.isPresent();
    }

    /**
     * For given user_id, check whether it is already in the database
     * @param db OrientDB TwitterDB connection
     * @param userId given user_id
     * @return true for User in database, false o.w.
     */
    public static boolean checkUserVertex(ODatabaseSession db, long userId) {
        String query1 = String.format("SELECT * FROM User WHERE user_id = %d", userId);
        OResultSet iter1 = db.query(query1);
        Stream<OVertex> userList = iter1.vertexStream();
        Optional<OVertex> optionUser = userList.findFirst();
        return optionUser.isPresent();
    }

    /**
     * For given hashtag name, check whether it is already in the database
     * @param db OrientDB TwitterDB connection
     * @param hashtag given hashtag name
     * @return true for Hashtag in database, false o.w.
     */
    public static boolean checkHashtagVertex(ODatabaseSession db, String hashtag) {
        String query1 = String.format("SELECT * FROM Hashtag WHERE hashtag = \"%s\"", hashtag);
        OResultSet iter1 = db.query(query1);
        Stream<OVertex> hashtagList = iter1.vertexStream();
        Optional<OVertex> hashtagVertex = hashtagList.findFirst();
        return hashtagVertex.isPresent();
    }

    /**
     * Convert specific time string to Date object
     * @param date specific time format for converting, "Weekday Month DD hh:mm:ss +xxxx YEAR"
     * @return Date object representing datetime
     */
    private static Date convertStringTimeToDate(String date) {
        String[] splits = date.split(" ");
        String dateFormat = "";
        // Year
        dateFormat += splits[splits.length - 1];
        dateFormat += "-";
        // Month
        String monthStr = splits[splits.length - 5];
        switch (monthStr) {
            case "Jan": dateFormat += "01";
                break;
            case "Feb": dateFormat += "02";
                break;
            case "Mar": dateFormat += "03";
                break;
            case "Apr": dateFormat += "04";
                break;
            case "May": dateFormat += "05";
                break;
            case "Jun": dateFormat += "06";
                break;
            case "Jul": dateFormat += "07";
                break;
            case "Aug": dateFormat += "08";
                break;
            case "Sep": dateFormat += "09";
                break;
            case "Oct": dateFormat += "10";
                break;
            case "Nov": dateFormat += "11";
                break;
            case "Dec": dateFormat += "12";
                break;
        }
        dateFormat += "-";
        // Date
        dateFormat += splits[splits.length - 4];
        dateFormat += " ";
        // Time
        dateFormat += splits[splits.length - 3];

//        System.out.println(dateFormat);
        SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        try {
            return dt.parse(dateFormat);
        } catch (java.text.ParseException e) {
            return new Date();
        }
    }

    /**
     * Actual JSON parsing function for extracting information and creating class objects for Vertex and Edges
     * Currently only extracting information
     * TODO: NEED LOGIC TO ADD VERTEX, EDGES
     * @param tweet JSON object for exactly one tweet from Kaggle data set
     */
    private static void handleEachTweet(ODatabaseSession db, JSONObject tweet) {
        // Weird lines in dataset, ignore
        if (tweet.get("limit") != null) {
            return;
        }

        // Get tweet_id
        long tweetId = (long) tweet.get("id");
        // Get user_id
        JSONObject user = (JSONObject) tweet.get("user");
        long userId = (long) user.get("id");

        // Get retweet tweet_id
        long retweetId = -1;
        JSONObject retweet = (JSONObject) tweet.get("retweeted_status");
        if (retweet != null) {
            retweetId = (long) retweet.get("id");
        }
        // Whether retweet already in the database
        if (retweet != null && !checkTweetVertex(db, retweetId)) {
            // Retweet not in database, need to add retweet Vertex to database for "retweet" edge

            // Get retweet create_at
            String rtCreateAtStr = (String) retweet.get("created_at");
            Date rtCreateAt = convertStringTimeToDate(rtCreateAtStr);
            // Get retweet text
            String rtText = (String) retweet.get("text");
            JSONObject rtUser = (JSONObject) retweet.get("user");
            // Get retweet user_id
            long rtUserId = (long) rtUser.get("id");
            // Get retweet geo
            String rtCountry = null;
            JSONObject rtGeo = (JSONObject) retweet.get("place");
            if (rtGeo != null) {
                rtCountry = (String) rtGeo.get("country");
            }
            // Retweet retweet_id and quote_id not presented, set to -1 for future update
            long rtRetweetId = -1;
            long rtQuoteId = -1;
            // Get retweet reply_id
            long rtReplyId = -1;
            if (retweet.get("in_reply_to_status_id") != null) {
                rtReplyId = (long) retweet.get("in_reply_to_status_id");
            }
            // Add retweet Vertex to database
            createTweetVertex(db, retweetId, rtCreateAt, rtText, rtUserId, rtCountry, rtRetweetId, rtReplyId, rtQuoteId);

            if (!checkUserVertex(db, rtUserId)) {
                // Retweet User not in database
                String screenName = (String) rtUser.get("screen_name");
                String country = null;
                if (rtUser.get("location") != null) {
                    country = (String) user.get("location");
                }
                boolean verified = (boolean) rtUser.get("verified");

                createUserVertex(db, rtUserId, screenName, country, verified);
            }
            // Add "post" Edge for this retweet
            createPostEdge(db, rtUserId, retweetId);

            // Add "reply" Edge if reply_id presented here
            if (rtReplyId != -1 && checkTweetVertex(db, rtReplyId)) {
                createReplyEdge(db, retweetId, rtReplyId);
            }
        } else if (retweet != null && checkTweetVertex(db, retweetId)) {
            // Retweet already in database
            createRetweetEdge(db, tweetId, retweetId);
        }
        // No retweet module

        // NOTE: retweet_id, reply_id, quote_id = -1 means no such retweet, reply, or quote
        long replyId = -1;
        if (tweet.get("in_reply_to_status_id") != null) {
            replyId = (long) tweet.get("in_reply_to_status_id");
        }
        long quoteId = -1;
        if (tweet.get("quoted_status_id") != null) {
            quoteId = (long) tweet.get("quoted_status_id");
        }

        if (checkTweetVertex(db, tweetId)) {
            // Tweet already in database, update retweet_id and quote_id, and insert corresponding "retweet", "quote" relationship
            if (replyId != -1 && checkTweetVertex(db, replyId)) {
                createReplyEdge(db, tweetId, replyId);

                String checkQuery = String.format("SELECT reply_id FROM Tweet WHERE tweet_id = %d", tweetId);
                OResultSet iter1 = db.query(checkQuery);
                Stream<OVertex> list = iter1.vertexStream();
                Optional<OVertex> reply = list.findFirst();
                if (reply.isPresent()) {
                    OVertex vert = reply.get();
                    long r_id = (long) vert.getProperty("reply_id");
                    if (r_id != replyId) {
                        String replyQuery = String.format("UPDATE Tweet SET reply_id = %d WHERE tweet_id = %d", replyId, tweetId);
                        db.query(replyQuery);
                    }
                }
            }
            if (quoteId != -1 && checkTweetVertex(db, quoteId)) {
                createQuoteEdge(db, tweetId, quoteId);

                String checkQuery = String.format("SELECT quote_id FROM Tweet WHERE tweet_id = %d", tweetId);
                OResultSet iter1 = db.query(checkQuery);
                Stream<OVertex> list = iter1.vertexStream();
                Optional<OVertex> quote = list.findFirst();
                if (quote.isPresent()) {
                    OVertex vert = quote.get();
                    long q_id = (long) vert.getProperty("quote_id");
                    if (q_id != quoteId) {
                        String quoteQuery = String.format("UPDATE Tweet SET quote_id = %d WHERE tweet_id = %d", quoteId, tweetId);
                        db.query(quoteQuery);
                    }
                }
            }
            return;
        }

        // Adding new Tweet Vertex
        if (!checkTweetVertex(db, tweetId)) {
            String createAtStr = (String) tweet.get("created_at");
            Date createAt = convertStringTimeToDate(createAtStr);

            String text = (String) tweet.get("text");

            // NOTE: country might be null
            String country = null;
            JSONObject geo = (JSONObject) tweet.get("place");
            if (geo != null) {
                country = (String) geo.get("country");
            }

            createTweetVertex(db, tweetId, createAt, text, userId, country, retweetId, replyId, quoteId);
        }

        // Adding new User Vertex
        if (!checkUserVertex(db, userId)) {
            String screenName = (String) user.get("screen_name");
            String country = null;
            if (user.get("location") != null) {
                country = (String) user.get("location");
            }
            boolean verified = (boolean) user.get("verified");

            createUserVertex(db, userId, screenName, country, verified);
        }

        // Adding new "post" Edge
        createPostEdge(db, userId, tweetId);

        // Adding new "retweet" Edge
        if (retweetId != -1) {
            createRetweetEdge(db, tweetId, retweetId);
        }

        // Adding new "reply" Edge
        if (replyId != -1) {
            createReplyEdge(db, tweetId, replyId);
        }

        // Adding new "quote" Edge
        if (quoteId != -1) {
            createQuoteEdge(db, tweetId, quoteId);
        }

        // Adding new Hashtag Vertex
        JSONObject entities = (JSONObject) tweet.get("entities");
        JSONArray fullHashtags = (JSONArray) entities.get("hashtags");
        for (Object fullHashtag : fullHashtags) {
            JSONObject hashtagInfo = (JSONObject) fullHashtag;
            String hashtag = (String) hashtagInfo.get("text");
            hashtag = Normalizer.normalize(hashtag, Normalizer.Form.NFD);
            hashtag = hashtag.replaceAll("\\p{M}", "");
//            System.out.println(hashtag);
            if (!checkHashtagVertex(db, hashtag)) {
                createHashtagVertex(db, hashtag);
            }
            // Adding new "contain" Edge
            createContainEdge(db, tweetId, hashtag);
        }
    }

//    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws IOException, ParseException {
        //OrientDB connection
        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        ODatabaseSession db = orient.open("twitterdb", "root", "sc16041102");

        //create schemas for Tweets and Users, create relations
        createTweetSchema(db);
        createUserSchema(db);
        createHashtagSchema(db);
        createRelations(db);

        /*
        * JSON parsing for local twitter data set
        * NOTE: The JSON data set it gave is not actually JSON
        *       It has weird eof in between each JSON object
        *       Therefore requires us to only read old lines, ex. 1, 3, 5, ...
        * If you want to read your file, please change the file reader filename
        */
        JSONParser jsonParser = new JSONParser();
        try (BufferedReader br = new BufferedReader(new FileReader("C:/Users/17479/Desktop/java_test/src/main/resources/data/Eurovision4.json"))) {
            int lineNum = 0;
            String line;
            while ((line = br.readLine()) != null) {
                lineNum++;
                if (lineNum % 2 == 0) {
                    continue;
                }
                JSONObject json = (JSONObject) jsonParser.parse(line);
                handleEachTweet(db, json);
            }
        }

        db.close();
        orient.close();
    }

}

//      Match {class: User, as: u1} -post-> {as: t1} -reply-> {as: t2} <-post- {as: u2, where: ($matched.u1 != $currentMatch)},
//	        {as: u2} -post-> {as: t3} -reply-> {as: t4} <-post- {as: u1}
//      RETURN u1.user_id, u2.user_id, t1.tweet_id, t2.tweet_id, t3.tweet_id, t4.tweet_id

//      Match {class: User, as: u1} -post-> {as: t1} -reply-> {as: t2} <-post- {as: u2, where: ($matched.u1 != $currentMatch)},
//          {as: u1} -post-> {as: t3} <-reply- {as: t4} <-post- {as: u2},

//          {as: u2} -post-> {as: t5} -reply-> {as: t6} <-post- {as: u3, where: ($matched.u2 != $currentMatch)},
//          {as: u2} -post-> {as: t7} <-reply- {as: t8} <-post- {as: u3},

//          {as: u3} -post-> {as: t9} -reply-> {as: t10} <-post- {as: u1},
//          {as: u3} -post-> {as: t11} <-reply- {as: t12} <-post- {as: u1}
//      RETURN u1.user_id, u2.user_id, u3.user_id, t1.tweet_id, t2.tweet_id, t3.tweet_id, t4.tweet_id, t5.tweet_id, t6.tweet_id, t7.tweet_id, t8.tweet_id, t9.tweet_id, t10.tweet_id, t11.tweet_id, t12.tweet_id