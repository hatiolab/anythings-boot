package xyz.anythings.boot.handler.idgen;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import xyz.anythings.base.event.EventConstants;
import xyz.anythings.base.event.main.IdGenerationEvent;
import xyz.anythings.sys.service.ICustomService;
import xyz.elidom.util.ValueUtil;

/**
 * ID 생성 이벤트 핸들러
 * 
 * @author shortstop
 */
@Component
public class IdGenerationEventHandler {
	/**
	 * 다이나믹 서비스
	 */
	@Autowired
	private ICustomService customService;
	/**
	 * 배치 ID 생성 커스텀 서비스 
	 */
	private static String BATCH_ID_GENERATE_CUSTOM_SERVICE = "diy-generate-batch-id";
	
	/**
	 * ID 생성 이벤트를 비동기로 처리
	 * 
	 * @param idGenEvent
	 */
	@EventListener(classes = IdGenerationEvent.class)
	public void handleIdGenerationEvent(IdGenerationEvent idGenEvent) {
		if(idGenEvent.getEventType() == EventConstants.EVENT_ID_GENERATION_BATCH_ID) {
			this.handleBatchIdGenerationEvent(idGenEvent);
		}
	}
	
	/**
	 * 배치 ID 생성 이벤트 처리
	 * 
	 * @param idGenEvent
	 */
	private void handleBatchIdGenerationEvent(IdGenerationEvent idGenEvent) {
		Object newId = this.customService.doCustomService(idGenEvent.getDomainId(), BATCH_ID_GENERATE_CUSTOM_SERVICE, ValueUtil.newMap("event", idGenEvent));
		
		if(ValueUtil.isNotEmpty(newId)) {
			idGenEvent.setResult(newId);
			idGenEvent.setAfterEventCancel(true);
		}
	}

}
