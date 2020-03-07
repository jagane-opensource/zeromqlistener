/*
 * Copyright (c) 2020, InfinStor, Inc.
 * Licensed under the terms of the Apache License Version 2.0, January 2004
 * See the file LICENSE-2.0.txt at the top of this repository
 */
package com.infinstor.bitcoin.zeromqlistener;

import java.util.Formatter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.ByteArrayInputStream;

import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import org.zeromq.ZMsg;
import org.zeromq.ZFrame;

import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.params.MainNetParams;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;

public class Listener {
  public static void main(String args[]) {

    if (args.length != 2) {
      System.err.println("Usage: Listener tcp-port s3-bucketname");
      System.exit(-1);
    }
    int portNum = 0;
    try {
      portNum = Integer.parseInt(args[0]);
    } catch (NumberFormatException nfe) {
      System.err.println("Caught " + nfe + " parsing " + args[0]);
      System.exit(-1);
    }
    String bucketname = args[1];

    AmazonS3 s3client = AmazonS3ClientBuilder.defaultClient();

    ZMQ.Context context = ZMQ.context(1);

    ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
    subscriber.connect("tcp://127.0.0.1:" + portNum);

    subscriber.subscribe("rawtx".getBytes());

    NetworkParameters params = MainNetParams.get();

    System.out.println("Subscribed to 0mq queue for rawtx message. Waiting...");
    while (true) {

      ZMsg zMsg = ZMsg.recvMsg(subscriber);
      int messageNumber = 0;
      for (ZFrame f: zMsg) {
        byte[] bytes = f.getData();
        if (messageNumber == 0) {
          String messageType = new String(bytes);
          System.out.println("Message type: " + messageType);
        } else if (messageNumber == 1) {
          System.out.println("Received raw tx of len=" + bytes.length);
          /* we write to the s3 bucket only if we can parse into a Transaction object */
          Transaction transaction = null;
          try {
            transaction = new Transaction(params, bytes);
          } catch (Exception ex) {
            System.err.println("Caught " + ex + " parsing bytes read into a Transaction object");
          }
          System.out.println("Transaction=" + transaction);
          if (transaction != null)
            writeToBucket(s3client, bucketname, bytes, transaction);
        }
        messageNumber++;
      }
    }
  }

  private static boolean writeToBucket(AmazonS3 s3client, String bucketname, byte[] bytes, Transaction transaction) {
    Sha256Hash hash = transaction.getTxId();
    if (hash == null)
      return false;
    String objectName = hash.toString();

    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd/kk");
    Date date = new Date();

    String key = "rawtransactions/" + formatter.format(date) + "/" + objectName;
    System.out.println("key=" + key);

    ObjectMetadata om = new ObjectMetadata();
    om.setContentLength(bytes.length);

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    try {
      s3client.putObject(bucketname, key, bais, om);
    } catch (Exception ex) {
      System.err.println("Caught " + ex + " putting object " + key + " in bucket " + bucketname);
    }

    return true;
  }

}
