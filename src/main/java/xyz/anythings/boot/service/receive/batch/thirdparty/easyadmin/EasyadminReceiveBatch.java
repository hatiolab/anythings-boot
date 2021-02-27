package xyz.anythings.boot.service.receive.batch.thirdparty.easyadmin;

import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import xyz.anythings.base.event.main.BatchReceiveEvent;

/**
 * Easyadmin 주문 수신 서비스
 * 
 * @author shortstop
 */
@Component
public class EasyadminReceiveBatch {

//	/**
//	 * 주문 수신 서머리 수신 후 커스텀 서비스
//	 */
//	private static final String DIY_HANDLE_SUMMARY_BY_EASYADMIN = "diy-handle-summary-by-easyadmin";
//	/**
//	 * 주문 수신 처리 커스텀 서비스
//	 */
//	private static final String DIY_HANDLE_ORDERS_BY_EASYADMIN = "diy-handle-orders-by-easyadmin";
//	/**
//	 * 주문 수신 완료 후 커스텀 서비스
//	 */
//	private static final String DIY_HANDLE_BATCHES_BY_EASYADMIN = "diy-handle-batches-by-easyadmin";
//	/**
//	 * NOSNOS 날짜 포맷
//	 */
//	private static final String NOSNOS_DATE_FORMAT = "yyyyMMdd";
//	/**
//	 * 쿼리 매니저
//	 */
//	@Autowired
//	private IQueryManager queryManager;
//	/**
//	 * 커스텀 서비스
//	 */
//	@Autowired
//	private ICustomService customService;
	
	/**
	 * 주문 정보 수신을 위한 수신 서머리 (배치 회차) 정보 조회
	 *  
	 * @param event
	 */
	@EventListener(classes = BatchReceiveEvent.class, condition = "#event.eventType == 10 and #event.eventStep == 1 and #event.thirdPartyProvider == 'easyadmin'")
	public void handleReadyToReceive(BatchReceiveEvent event) {
	}

	/**
	 * 주문 정보 수신
	 *  
	 * @param event
	 */
	@EventListener(classes = BatchReceiveEvent.class, condition = "#event.eventType == 20 and #event.eventStep == 1 and #event.thirdPartyProvider == 'easyadmin'")
	public void handleStartToReceive(BatchReceiveEvent event) { 

	}
	
	/*************************************************************************************************/
	/*											수 신 로 직  											 */
	/*************************************************************************************************/
}
