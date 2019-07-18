package xyz.anythings.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import xyz.anythings.sys.event.EventPublisher;
import xyz.anythings.sys.event.model.AppsEvent;
import xyz.elidom.util.BeanUtil;

@EnableAsync
@EnableScheduling
@SpringBootApplication
@EnableConfigurationProperties
@ComponentScan(basePackages = { "xyz.anythings.*", "xyz.elidom.*" })
@ImportResource({ "classpath:/WEB-INF/application-context.xml", "classpath:/WEB-INF/dataSource-context.xml" })
public class AnythingsBootApplication {
	
	public static void main(String[] args) {
		SpringApplication.run(AnythingsBootApplication.class, args);
		
		AppsEvent event = new AppsEvent();
		event.setAppsStatus("started");
		
		BeanUtil.get(EventPublisher.class).publishEvent(event);
	}
}