/* Copyright © HatioLab Inc. All rights reserved. */
package xyz.anythings.boot.web.initializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import xyz.anythings.boot.config.ModuleProperties;
import xyz.elidom.sys.config.ModuleConfigSet;
import xyz.elidom.sys.system.config.module.IModuleProperties;
import xyz.elidom.sys.system.service.api.IEntityFieldCache;
import xyz.elidom.sys.system.service.api.IServiceFinder;

/**
 * Anythings Boot 모듈 Startup시 Framework 초기화 클래스
 * 
 * @author yang
 */
@Component
public class AnythingsBootInitializer {

	/**
	 * Logger
	 */
	private Logger logger = LoggerFactory.getLogger(AnythingsBootInitializer.class);

	@Autowired
	@Qualifier("rest")
	private IServiceFinder restFinder;

	@Autowired
	private IEntityFieldCache entityFieldCache;

	@Autowired
	private ModuleProperties module;

	@Autowired
	private ModuleConfigSet configSet;
	
	@EventListener({ ContextRefreshedEvent.class })
	public void refresh(ContextRefreshedEvent event) {
		this.logger.info("Anythings Boot module refreshing...");
		
		this.setupApplicationModule();
		
		this.logger.info("Anythings Boot module refreshed!");
	}

	@EventListener({ ApplicationReadyEvent.class })
	void ready(ApplicationReadyEvent event) {
		this.logger.info("Anythings Boot module initializing...");
		
		this.setupApplicationModule();
		this.scanServices();
		
		this.logger.info("Anythings Boot module initialized!");
	}
	
	/**
	 * 애플리케이션 메인 모듈 셋업
	 */
	private void setupApplicationModule() {
		IModuleProperties mainModule = this.configSet.getApplicationModule();
		if(mainModule == null) {
			this.configSet.addConfig(this.module.getName(), this.module);
			this.configSet.setApplicationModule(this.module.getName());
		}
	}

	/**
	 * 모듈 서비스 스캔
	 */
	private void scanServices() {
		this.entityFieldCache.scanEntityFieldsByBasePackage(this.module.getBasePackage());
		this.restFinder.scanServicesByPackage(this.module.getName(), this.module.getBasePackage());
	}
}