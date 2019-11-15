/* Copyright © HatioLab Inc. All rights reserved. */
package xyz.anythings.boot.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import xyz.elidom.sys.system.config.module.IModuleProperties;
import xyz.elidom.util.FormatUtil;

/**
 * 샘플 Boot 모듈 정보 파일
 * 
 * @author yang
 */
@Component("anythingsBootModuleProperties")
@EnableConfigurationProperties
@PropertySource("classpath:/properties/anythings-boot.properties")
public class ModuleProperties implements IModuleProperties {

	/**
	 * 모듈명
	 */
	@Value("${anythings.boot.name}")
	private String name;

	/**
	 * 버전
	 */
	@Value("${anythings.boot.version}")
	private String version;

	/**
	 * Module Built Time
	 */
	@Value("${anythings.boot.built.at}")
	private String builtAt;

	/**
	 * 모듈 설명
	 */
	@Value("${anythings.boot.description}")
	private String description;

	/**
	 * 부모 모듈
	 */
	@Value("${anythings.boot.parentModule}")
	private String parentModule;

	/**
	 * 모듈 Scada Package
	 */
	@Value("${anythings.boot.basePackage}")
	private String basePackage;

	/**
	 * Scan Service Path
	 */
	@Value("${anythings.boot.scanServicePackage}")
	private String scanServicePackage;

	/**
	 * Scan Entity Path
	 */
	@Value("${anythings.boot.scanEntityPackage}")
	private String scanEntityPackage;
	
	/**
	 * Project Name
	 * 
	 * @return
	 */
	@Value("${anythings.boot.projectName}")
	private String projectName;

	public String getName() {
		return this.name;
	}

	public String getVersion() {
		return this.version;
	}

	public String getBuiltAt() {
		return builtAt;
	}

	public String getDescription() {
		return this.description;
	}

	public String getParentModule() {
		return this.parentModule;
	}

	public String getBasePackage() {
		return this.basePackage;
	}

	public String getScanServicePackage() {
		return this.scanServicePackage;
	}

	public String getScanEntityPackage() {
		return this.scanEntityPackage;
	}

	public String getProjectName() {
		return this.projectName;
	}
	
	@Override
	public String toString() {
		return FormatUtil.toJsonString(this);
	}
}