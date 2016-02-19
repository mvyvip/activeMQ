package com.mvyvip.mq.test;

import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

public class QueueTest {
	@Test
	public void testSender() throws Exception {
		ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://192.168.0.200:61616");
		
		Connection connection = factory.createConnection();
		
		Session session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
		
		Destination destination = session.createQueue("queue04");
		
		MessageProducer producer = session.createProducer(destination);
		
		for (int i = 0; i < 3; i++) {
			MapMessage mapMessage = session.createMapMessage();
			mapMessage.setString("key" + i, "setString" + i);
			mapMessage.setStringProperty("key" + i, "setStringProperty" + i);
			producer.send(mapMessage);
		}
		
		session.commit();
		
		session.close();
		connection.close();
	}
	
	@Test
	public void testSender2() throws Exception {
		ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://192.168.0.200:61616");
		
		Connection connection = factory.createConnection();
		
		Session session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
		
		Destination destination = session.createQueue("queue03");
		
		MessageProducer producer = session.createProducer(destination);
		
		for (int i = 0; i < 3; i++) {
			TextMessage textMessage = session.createTextMessage("textMessage:" + i);
			producer.send(textMessage);
		}
		
		session.commit();
		
		session.close();
		connection.close();
	}
	
	@Test
	public void testSender3() throws Exception {
		ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://192.168.0.200:61616");
		
		Connection connection = factory.createConnection();
		
		Session session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
		
		Destination destination = session.createQueue("queue04");
		
		MessageProducer producer = session.createProducer(destination);
		
		for (int i = 0; i < 3; i++) {
			MapMessage mapMessage = session.createMapMessage();
			mapMessage.setString("key" + i, "setString" + i);
			mapMessage.setStringProperty("key" + i, "setStringProperty" + i);
			producer.send(mapMessage);
		}
		
		session.commit();
		
		session.close();
		connection.close();
	}
	
	@Test
	public void testReceive() throws Exception {
		ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://192.168.0.200:61616");
		
		Connection connection = factory.createConnection();
		connection.start();
		
		Enumeration jmsxPropertys = connection.getMetaData().getJMSXPropertyNames();
		while(jmsxPropertys.hasMoreElements()) {
			System.out.println(jmsxPropertys.nextElement());
		}
		
		
		Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
													   
		Destination destination = session.createQueue("queue04");
		
		MessageConsumer consumer = session.createConsumer(destination);
		
		for (int i = 0; i < 3; i++) {
			MapMessage message = (MapMessage) consumer.receive();
			session.commit();
			System.out.println("getStringProperty" + i + " : " + message.getStringProperty("key" + i));
			System.out.println("getString" + i + " : " + message.getString("key" + i));
		}
		
		session.close();
		connection.close();
	}
	
	@Test
	public void testReceive2() throws Exception {
		ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://192.168.0.200:61616");
		
		Connection connection = factory.createConnection();
		connection.start();
		
		Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
													   
		Destination destination = session.createQueue("queue03");
		
		MessageConsumer consumer = session.createConsumer(destination);
		
		for (int i = 0; i < 3; i++) {
			TextMessage message = (TextMessage) consumer.receive();
			session.commit();
			System.out.println("key" + i + " : " + message.getText());
		}
		
		session.close();
		connection.close();
	}
	
	
	@Test
	public void testReceive3() throws Exception {
		ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://192.168.0.200:61616");
		
		Connection connection = factory.createConnection();
		connection.start();
		
		Enumeration jmsxPropertys = connection.getMetaData().getJMSXPropertyNames();
		while(jmsxPropertys.hasMoreElements()) {
			System.out.println(jmsxPropertys.nextElement());
		}
		
		
		Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
													   
		Destination destination = session.createQueue("queue04");
		
		MessageConsumer consumer = session.createConsumer(destination);
		
		for (int i = 0; i < 3; i++) {
			MapMessage message = (MapMessage) consumer.receive();
//			session.commit();
			System.out.println("getStringProperty" + i + " : " + message.getStringProperty("key" + i));
			System.out.println("getString" + i + " : " + message.getString("key" + i));
			if(i == 2) {
				message.acknowledge();
			}
		}
		
		session.close();
		connection.close();
	}
	
	
	
	
	
}
