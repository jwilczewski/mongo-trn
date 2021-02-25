package com.consdata.test.mongo.trn;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.IntStream;

@Slf4j
public class TestSingleDocument {

    public static void main(String[] args) {
        Random random = new Random();

        Runnable transfer = () -> {
            LOGGER.info("start transfering");
            MongoClient mongoClient = new MongoClient();
            MongoCollection<Document> users = mongoClient.getDatabase("trntest").getCollection("users");
            IntStream.range(0, 10000).forEach(i -> {
                int modify = random.nextInt(100);
                users.updateOne(new Document("login", "jkowalski"), new Document("$inc", new Document(Map.of(
                        "wallet.subAccountA", modify,
                        "wallet.subAccountB", -1 * modify))));
            });
            LOGGER.info("stop transfering");
            mongoClient.close();
        };

        Runnable check = () -> {
            LOGGER.info("start checking");
            MongoClient mongoClient = new MongoClient();
            MongoCollection<Document> users = mongoClient.getDatabase("trntest").getCollection("users");
            IntStream.range(0, 100000).forEach(i -> {
                FindIterable<Document> documents = users.find(new Document("login", "jkowalski"));
                Document wallet = Objects.requireNonNull(documents.first()).get("wallet", Document.class);
                Integer subAccountA = wallet.getInteger("subAccountA");
                Integer subAccountB = wallet.getInteger("subAccountB");
                if (subAccountA + subAccountB != 2000000) {
                    LOGGER.error("ERROR");
                }
            });
            LOGGER.info("stop checking");
            mongoClient.close();
        };

        IntStream.range(0, 10).forEach(i -> {
            Thread t = new Thread(transfer);
            Thread c = new Thread(check);
            t.start();
            c.start();
        });
    }
}
