package com.consdata.test.mongo.trn;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.Objects;
import java.util.Random;
import java.util.stream.IntStream;

@Slf4j
public class TestMultipleDocument {

    public static void main(String[] args) {
        Random random = new Random();
        long t1 = System.currentTimeMillis();

        Runnable transfer = () -> {
            LOGGER.info("start transfering");
            MongoClient mongoClient = new MongoClient();
            MongoCollection<Document> users = mongoClient.getDatabase("trntest").getCollection("users");
            IntStream.range(0, 10000).forEach(i -> {
                int modify = random.nextInt(100);
                users.updateOne(new Document("login", "jkowalski"), new Document("$inc", new Document("wallet.subAccountA", modify)));
                users.updateOne(new Document("login", "anowak"), new Document("$inc", new Document("wallet.subAccountA", -1 * modify)));
            });
            LOGGER.info("stop transfering");
            mongoClient.close();
            LOGGER.info("time: {}", System.currentTimeMillis() - t1);
        };

        Runnable check = () -> {
            LOGGER.info("start checking");
            MongoClient mongoClient = new MongoClient();
            MongoCollection<Document> users = mongoClient.getDatabase("trntest").getCollection("users");
            IntStream.range(0, 10000).forEach(i -> {
                FindIterable<Document> documentsKowalski = users.find(new Document("login", "jkowalski"));
                Document walletKowalski = Objects.requireNonNull(documentsKowalski.first()).get("wallet", Document.class);
                Integer subAccountA = walletKowalski.getInteger("subAccountA");

                FindIterable<Document> documentsNowak = users.find(new Document("login", "anowak"));
                Document walletNowak = Objects.requireNonNull(documentsNowak.first()).get("wallet", Document.class);
                Integer subAccountB = walletNowak.getInteger("subAccountA");
                if (subAccountA + subAccountB != 200000) {
                    LOGGER.error("ERROR sum: {}", subAccountA + subAccountB);
                } else {
                    LOGGER.info("checked");
                }
            });
            LOGGER.info("stop checking");
            mongoClient.close();
            LOGGER.info("time: {}", System.currentTimeMillis() - t1);
        };

        IntStream.range(0, 10).forEach(i -> {
            Thread t = new Thread(transfer);
            Thread c = new Thread(check);
            t.start();
            c.start();
        });
    }
}
