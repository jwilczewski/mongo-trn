package com.consdata.test.mongo.trn;

import com.mongodb.*;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.TransactionBody;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.Objects;
import java.util.Random;
import java.util.stream.IntStream;

@Slf4j
public class TestMultipleDocumentTransactional {

    public static void main(String[] args) {
        Random random = new Random();
        long t1 = System.currentTimeMillis();

        TransactionOptions txnOptions = TransactionOptions.builder()
                .readPreference(ReadPreference.primary())
                .readConcern(ReadConcern.LOCAL)
                .writeConcern(WriteConcern.MAJORITY)
                .build();

        Runnable transfer = () -> {
            LOGGER.info("start transfering");
            MongoClient mongoClient = new MongoClient();

            IntStream.range(0, 10000).forEach(i -> {
                final ClientSession clientSession = mongoClient.startSession();
                TransactionBody<String> txnBody = () -> {
                    MongoCollection<Document> users = mongoClient.getDatabase("trntest").getCollection("users");
                    int modify = random.nextInt(100);
                    users.updateOne(clientSession, new Document("login", "jkowalski"), new Document("$inc", new Document("wallet.subAccountA", modify)));
                    users.updateOne(clientSession, new Document("login", "anowak"), new Document("$inc", new Document("wallet.subAccountA", -1 * modify)));
                    return "updated";
                };
                try {
                    clientSession.withTransaction(txnBody, txnOptions);
                } catch (RuntimeException e) {
                    LOGGER.error("ERROR", e);
                } finally {
                    clientSession.close();
                }
            });
            LOGGER.info("stop transfering");
            mongoClient.close();
            LOGGER.info("time: {}", System.currentTimeMillis() - t1);
        };

        Runnable check = () -> {
            LOGGER.info("start checking");
            MongoClient mongoClient = new MongoClient();

            IntStream.range(0, 10000).forEach(i -> {
                final ClientSession clientSession = mongoClient.startSession();
                TransactionBody<String> txnBody = () -> {
                    MongoCollection<Document> users = mongoClient.getDatabase("trntest").getCollection("users");

                    FindIterable<Document> documentsK = users.find(clientSession, new Document("login", "jkowalski"));
                    Document walletKowalski = Objects.requireNonNull(documentsK.first()).get("wallet", Document.class);
                    Integer subAccountAKowalski = walletKowalski.getInteger("subAccountA");

                    FindIterable<Document> documentsN = users.find(clientSession, new Document("login", "anowak"));
                    Document walletNowak = Objects.requireNonNull(documentsN.first()).get("wallet", Document.class);
                    Integer subAccountANowak = walletNowak.getInteger("subAccountA");

                    if (subAccountAKowalski + subAccountANowak != 200000) {
                        LOGGER.error("ERROR sum: {}", subAccountAKowalski + subAccountANowak);
                    } else {
                        LOGGER.info("checked");
                    }

                    return "checked";
                };
                try {
                    clientSession.withTransaction(txnBody, txnOptions);
                } catch (RuntimeException e) {
                    LOGGER.error("ERROR", e);
                } finally {
                    clientSession.close();
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
