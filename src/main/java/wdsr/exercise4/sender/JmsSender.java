package wdsr.exercise4.sender;

import java.math.BigDecimal;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.Order;

public class JmsSender {
	private static final Logger log = LoggerFactory.getLogger(JmsSender.class);
	
	private final String queueName;
	private final String topicName;
	
	private final String connectionUri = "tcp://localhost:62616";
	private ActiveMQConnectionFactory connectionFactory; 
	private Connection connection;
	private Session session;

	public JmsSender(final String queueName, final String topicName) {
		this.queueName = queueName;
		this.topicName = topicName;
		
		try {
			setUp();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	public void setUp() throws JMSException{
		connectionFactory = new ActiveMQConnectionFactory(connectionUri); 
		connection = connectionFactory.createConnection();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE); 
	}
	
	/**
	 * This method creates an Order message with the given parameters and sends it as an ObjectMessage to the queue.
	 * @param orderId ID of the product
	 * @param product Name of the product
	 * @param price Price of the product
	 */
	public void sendOrderToQueue(final int orderId, final String product, final BigDecimal price) {
		Order order = new Order(orderId, product, price);
		try {
			sendMessage(order);
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	private void sendMessage(Order order) throws JMSException {
        Destination destination = session.createQueue(queueName); 
        MessageProducer producer = session.createProducer(destination);
        ObjectMessage message = session.createObjectMessage(order);
        message.setJMSType("Order");
        message.setStringProperty("WDSR-System", "OrderProcessor");
        producer.send(message);
        connection.close(); 
	}

	/**
	 * This method sends the given String to the queue as a TextMessage.
	 * @param text String to be sent
	 */
	public void sendTextToQueue(String text) {
		try {
			sendMessage(text);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void sendMessage(String text) throws Exception{
        Destination destination = session.createQueue(queueName); 
        MessageProducer producer = session.createProducer(destination);
        TextMessage message = session.createTextMessage(text);
        producer.send(message);
        connection.close(); 
	}

	/**
	 * Sends key-value pairs from the given map to the topic as a MapMessage.
	 * @param map Map of key-value pairs to be sent.
	 */
	public void sendMapToTopic(Map<String, String> map) {
		try {
			sendMessage(map);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void sendMessage(Map<String, String> map) throws JMSException{
        Destination destination = session.createTopic(topicName); 
        MessageProducer producer = session.createProducer(destination);
		MapMessage message = session.createMapMessage();

		for (Map.Entry<String, String> entry : map.entrySet()) {
			message.setString(entry.getKey(), entry.getValue());
		}

        producer.send(message);
        connection.close(); 
	}
}