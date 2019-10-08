package xyz.anythings.boot.receiver;

import org.springframework.context.event.EventListener;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import xyz.elidom.rabbitmq.client.event.SystemMessageReceiveEvent;

/**
 * 미들웨어 리시버 샘플 
 * @author yang
 *
 */
@Component
public class MwReceiver {
	@EventListener(condition = "#event.queueName == @anythingsBoot.getRabbitmqQueue(), #event.isExecuted() == false")
	@Order(Ordered.LOWEST_PRECEDENCE)
	public void bootMessageReceiver(SystemMessageReceiveEvent event) {
	}
	
	
	@EventListener(condition = "#event.queueName == @anythingsSysModuleProperties.getRabbitmqQueue(), #event.isExecuted() == false")
	@Order(Ordered.LOWEST_PRECEDENCE)
	public void sysMessageReceiver(SystemMessageReceiveEvent event) {
	}

	
	@EventListener(condition = "#event.queueName == @anythingsLogisBaseModuleProperties.getRabbitmqQueue(), #event.isExecuted() == false")
	@Order(Ordered.LOWEST_PRECEDENCE)
	public void logisMessageReceiver(SystemMessageReceiveEvent event) {
	}

}
