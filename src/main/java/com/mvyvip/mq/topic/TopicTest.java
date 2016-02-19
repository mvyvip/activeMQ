package com.mvyvip.mq.topic;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

public class TopicTest {
	/**
	 * 非持久化发送
	 * @throws Exception
	 */
	@Test
	public void testNoPersistenceSender() throws Exception {
		ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://192.168.0.200:61616");
		Connection connection = factory.createConnection();
		connection.start();
		
		Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
		
		Topic topic = session.createTopic("topic01");
		MessageProducer producer = session.createProducer(topic);
		for (int i = 0; i < 3; i++) {
			TextMessage textMessage = session.createTextMessage("testNoPersistenceSender:" + i);
			producer.send(textMessage);
		}
		
		session.commit();
		connection.close();
	}
	
	/**
	 * 非持久接收
	 * @throws Exception
	 */
	@Test
	public void testNoPersistenceReceive() throws Exception {
		ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://192.168.0.200:61616");
		Connection connection = factory.createConnection();
		connection.start();
		
		Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
		
		Topic topic = session.createTopic("topic01");
		MessageConsumer consumer = session.createConsumer(topic);

		for (int i = 0; i < 3; i++) {
			TextMessage textMessage = (TextMessage) consumer.receive();
			System.out.println(textMessage.getText());
		}
		
		session.commit();
		connection.close();
	}
	
	
	/**
	 * 持久化发送
	 * @throws Exception
	 */
	@Test
	public void testPersistenceSender() throws Exception {
		ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://192.168.0.200:61616");
		Connection connection = factory.createConnection();
		
		Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
		
		Topic topic = session.createTopic("topic02");
		MessageProducer producer = session.createProducer(topic);
		
		producer.setDeliveryMode(DeliveryMode.PERSISTENT);
		connection.start();
		for (int i = 0; i < 3; i++) {
			TextMessage textMessage = session.createTextMessage("testNoPersistenceSender:" + i);
			producer.send(textMessage);
		}
		
		session.commit();
		connection.close();
	}
	
	/**
	 * 持久化接收
	 * @throws Exception
	 */
	@Test
	public void testPersistenceReceive() throws Exception {
		ConnectionFactory factory = new ActiveMQConnectionFactory("tcp://192.168.0.200:61616");
		Connection connection = factory.createConnection();
		connection.setClientID("cli01");
		
		Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
		
		Topic topic = session.createTopic("topic02");
		TopicSubscriber consumer = session.createDurableSubscriber(topic, "t1");
		connection.start();

		for (int i = 0; i < 3; i++) {
			TextMessage textMessage = (TextMessage) consumer.receive();
			System.out.println("testPersistenceReceive --> " + textMessage.getText());
		}
		
		session.commit();
		connection.close();
	}
}
