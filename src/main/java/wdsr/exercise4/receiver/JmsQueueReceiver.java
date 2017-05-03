package wdsr.exercise4.receiver;

import java.math.BigDecimal;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.PriceAlert;
import wdsr.exercise4.VolumeAlert;

/**
 * TODO Complete this class so that it consumes messages from the given queue and invokes the registered callback when an alert is received.
 * 
 * Assume the ActiveMQ broker is running on tcp://localhost:62616
 */
public class JmsQueueReceiver {
	private static final Logger log = LoggerFactory.getLogger(JmsQueueReceiver.class);
	
	private final String connectionUri = "tcp://localhost:62616";
	private ActiveMQConnectionFactory connectionFactory; 
    private Connection connection; 
    private Session session; 
    private Destination destination; 
    private AlertService alertService;
	
	/**
	 * Creates this object
	 * @param queueName Name of the queue to consume messages from.
	 */
	public JmsQueueReceiver(final String queueName) {
		connectionFactory = new ActiveMQConnectionFactory(connectionUri); 
		connectionFactory.setTrustAllPackages(true);
        try {
			connection = connectionFactory.createConnection();
			connection.start(); 
	        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE); 
	        destination = session.createQueue(queueName);
		} catch (JMSException e) {
			e.printStackTrace();
		} 
        
	}

	/**
	 * Registers the provided callback. The callback will be invoked when a price or volume alert is consumed from the queue.
	 * @param alertService Callback to be registered.
	 */
	public void registerCallback(AlertService alertService) {
		 try {
			MessageConsumer consumer = session.createConsumer(destination);
			this.alertService = alertService;
			consumer.setMessageListener(new EventListener()); 
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Deregisters all consumers and closes the connection to JMS broker.
	 */
	public void shutdown() {
		if (connection != null) { 
            try {
				connection.close();
			} catch (JMSException e) {
				e.printStackTrace();
			} 
        } 
	}
	
	public class EventListener implements MessageListener{
		@Override
		public void onMessage(Message message) { 
		
			if(message instanceof ObjectMessage){
				ObjectMessage objectMessage = (ObjectMessage) message;
				callAlertService(objectMessage);
			}else if(message instanceof TextMessage){
				TextMessage textMessage = (TextMessage) message;
				callAlertService(textMessage);
			}
		
	}
		
		private void callAlertService(ObjectMessage objectMessage){	
			try {
					if(objectMessage.getJMSType().equals("PriceAlert")){
						PriceAlert priceAlert = (PriceAlert) objectMessage.getObject();
						alertService.processPriceAlert(priceAlert);
					}else if(objectMessage.getJMSType().equals("VolumeAlert")){
						VolumeAlert volumeAlert = (VolumeAlert) objectMessage.getObject();
						alertService.processVolumeAlert(volumeAlert);
					}
				} catch (JMSException e) {
					e.printStackTrace();
				}
		}
		
		private void callAlertService(TextMessage textMessage){
			try {
				String text = textMessage.getText();
				String[] values = retrieveValuesFromString(text); 
				String type = textMessage.getJMSType();
				
				if(type.equals("PriceAlert")){
					alertService.processPriceAlert(new PriceAlert(Long.valueOf(values[0]), values[1], new BigDecimal(values[2])));
				}else if(type.equals("VolumeAlert")){
					alertService.processVolumeAlert(new VolumeAlert(Long.valueOf(values[0]), values[1], Long.valueOf(values[2])));
				}
				
			} catch (JMSException e) {
				e.printStackTrace();
			}
			
		}
		
		private String[] retrieveValuesFromString(String input){
			String[] splited = input.split("\\r?\\n");
			String[] result = new String[3];
			
			for (int i = 0; i < splited.length; i++) {
				result[i] = splited[i].substring(splited[i].indexOf('=') + 1).trim();	
			}
			return result;
		}
		
		
}

}