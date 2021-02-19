package xyz.anythings.boot.service.receive.batch.thirdparty.nosnos;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import operato.logis.wcs.entity.Wave;
import xyz.anythings.base.LogisConstants;
import xyz.anythings.base.entity.JobBatch;
import xyz.anythings.base.event.main.BatchReceiveEvent;
import xyz.anythings.sys.service.ICustomService;
import xyz.elidom.dbist.dml.Page;
import xyz.elidom.exception.server.ElidomRuntimeException;
import xyz.elidom.orm.IQueryManager;
import xyz.elidom.sys.SysConstants;
import xyz.elidom.sys.util.SettingUtil;
import xyz.elidom.sys.util.ThrowUtil;
import xyz.elidom.util.BeanUtil;
import xyz.elidom.util.DateUtil;
import xyz.elidom.util.ValueUtil;

/**
 * NosNos WMS 주문 수신 서비스
 * 
 * @author shortstop
 */
@Component
public class NosNosReceiveBatch {
	
	/**
	 * 주문 수신 서머리 수신 후 커스텀 서비스
	 */
	private static final String DIY_HANDLE_SUMMARY_BY_NOSNOS = "diy-handle-summary-by-nosnos";
	/**
	 * 주문 수신 처리 커스텀 서비스
	 */
	private static final String DIY_HANDLE_ORDERS_BY_NOSNOS = "diy-handle-orders-by-nosnos";
	/**
	 * 주문 수신 완료 후 커스텀 서비스
	 */
	private static final String DIY_HANDLE_BATCHES_BY_NOSNOS = "diy-handle-batches-by-nosnos";
	/**
	 * NOSNOS 날짜포맷
	 */
	private static final String NOSNOS_DATE_FORMAT = "yyyyMMdd";
	/**
	 * 쿼리 매니저
	 */
	@Autowired
	private IQueryManager queryManager;
	/**
	 * 커스텀 서비스
	 */
	@Autowired
	private ICustomService customService;
	
	/**
	 * 주문 정보 수신을 위한 수신 서머리 (배치 회차) 정보 조회
	 *  
	 * @param event
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@EventListener(classes = BatchReceiveEvent.class, condition = "#event.eventType == 10 and #event.eventStep == 1 and #event.thirdPartyProvider == 'nosnos'")
	public void handleReadyToReceive(BatchReceiveEvent event) {
		// 1. 배치 서머리 (회차 정보) 수신
		Map<String, Object> summary = this.receiveBatchSummary(event.getJobDate());
		
		// 2. 수신할 회차 정보 추출
		Map<String, Object> response = (Map)this.parseApiResult(summary, null);
		List<Map> batchSeqList = (List<Map>)this.parseDateList(response, "shipping_info", "수신할 회차 정보가 없습니다.");
		
		// 3. 작업 차수 파싱
		List<String> jobSeqList = new ArrayList<String>();
		for(Map batchSeq : batchSeqList) {
			jobSeqList.add(ValueUtil.toString(batchSeq.get("seq")));
		}
		
		// 4. 이벤트에 수신할 회차 정보 결과 설정
		event.setResult(jobSeqList);
		
		// 5. 커스텀 서비스 호출
		Map<String, Object> parameters = ValueUtil.newMap("event", event);
		this.customService.doCustomService(event.getDomainId(), DIY_HANDLE_SUMMARY_BY_NOSNOS, parameters);
		
		// 6. 주문 수신
		this.handleStartToReceive(event);
		
		// 7. 이벤트 처리
		event.setExecuted(true);
	}

	/**
	 * 주문 정보 수신
	 *  
	 * @param event
	 */
	@SuppressWarnings({ "unchecked" })
	@EventListener(classes = BatchReceiveEvent.class, condition = "#event.eventType == 20 and #event.eventStep == 1 and #event.thirdPartyProvider == 'nosnos'")
	public void handleStartToReceive(BatchReceiveEvent event) { 
		// 1. 회차 정보
		List<String> jobSeqList = (List<String>)event.getResult();
		
		// 2. 회차 정보 중에 이마 수신 받은 차수를 배치에서 찾아서 있으면 제거
		Map<String, Object> params = ValueUtil.newMap("domainId,jobDate,jobSeqList,comCd", event.getDomainId(), event.getJobDate(), jobSeqList, event.getComCd());
		String sql = "select job_seq from waves where domain_id = :domainId and job_date = :jobDate and job_seq in (:jobSeqList) #if($comCd) and com_cd = :comCd #end";
		List<String> receivedJobSeqList = this.queryManager.selectListBySql(sql, params, String.class, 0, 0);
		
		// 3. 수신할 차수 필터링
		for(String jobSeq : receivedJobSeqList) {
			jobSeqList.remove(jobSeq);
		}
		
		if(ValueUtil.isEmpty(jobSeqList)) {
			throw ThrowUtil.newValidationErrorWithNoLog("수신할 회차 정보가 없습니다.");
		}
		
		// 4. 새로운 회차에 대해서 주문 수신 시작
		List<Wave> receivedWaves = new ArrayList<Wave>();
		NosNosReceiveBatch self = BeanUtil.get(NosNosReceiveBatch.class);
		
		for(String jobSeq : jobSeqList) {
			int waveSeq = ValueUtil.toInteger(jobSeq);
			Map<String, Object> response = this.receiveOrdersByJobSeq(event.getJobDate(), waveSeq, 1);
			
			// 별도 트랜잭션으로 처리 필요 ...
			Wave wave = self.configureWaveByOrders(event, waveSeq, response);
			
			if(wave != null) {
				receivedWaves.add(wave);
			}
		}
		
		if(receivedWaves.isEmpty()) {
			throw ThrowUtil.newValidationErrorWithNoLog("수신할 주문 정보가 없습니다.");
		}
	}
	
	/*************************************************************************************************/
	/*											수 신 로 직  											 */
	/*************************************************************************************************/

	/**
	 * 수신 받을 주문 서머리 (회차 정보)를 조회
	 * 
	 * @param jobDate
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map<String, Object> receiveBatchSummary(String jobDate) {
		jobDate = jobDate.replace(LogisConstants.DASH, LogisConstants.EMPTY_STRING);
		RestTemplate rest = new RestTemplate();
		rest.getMessageConverters().add(0, new StringHttpMessageConverter(Charset.forName(SysConstants.CHAR_SET_UTF8)));
		String apiBaseUrl = SettingUtil.getValue("3rdparty.wms.nosnos.api-base-url", "http://api.nosnos.kr");
		String batchSeqUrl = apiBaseUrl + "/v1/shipping/info?start_dt=" + jobDate + "&end_dt=" + jobDate;
		
		try {
			String reqDate = DateUtil.todayStr(NOSNOS_DATE_FORMAT);
			HttpEntity<?> requestEntity = this.createAuthHeaders(reqDate);
			ResponseEntity<Map> response = rest.exchange(batchSeqUrl, HttpMethod.GET, requestEntity, Map.class);
			return response.getBody();
		
		} catch(HttpClientErrorException hcee) {
			String error = hcee.getResponseBodyAsString();
			throw new ElidomRuntimeException("RECEIVE_BATCH_SUMMARY_ERROR", error, hcee);
			
		} catch(Exception e) {
			throw new ElidomRuntimeException("RECEIVE_BATCH_SUMMARY_ERROR", e.getMessage(), e);
		}
	}
	
	/**
	 * 작업 일자, 작업 차수에 대한 주문 수신 
	 * 
	 * @param jobDate
	 * @param jobSeq
	 * @param page
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map<String, Object> receiveOrdersByJobSeq(String jobDate, int jobSeq, int page) {
		jobDate = jobDate.replace(LogisConstants.DASH, LogisConstants.EMPTY_STRING);
		RestTemplate rest = new RestTemplate();
		rest.getMessageConverters().add(0, new StringHttpMessageConverter(Charset.forName(SysConstants.CHAR_SET_UTF8)));
		String apiBaseUrl = SettingUtil.getValue("3rdparty.wms.nosnos.api-base-url", "http://api.nosnos.kr");
		String batchSeqUrl = apiBaseUrl + "/v1/shipping/releases?order_date=" + jobDate + "&order_seq=" + jobSeq + "&release_status=0&page=" + page;
		
		try {
			String reqDate = DateUtil.todayStr(NOSNOS_DATE_FORMAT);
			HttpEntity<?> requestEntity = this.createAuthHeaders(reqDate);
			ResponseEntity<Map> response = rest.exchange(batchSeqUrl, HttpMethod.GET, requestEntity, Map.class);
			return response.getBody();
		
		} catch(HttpClientErrorException hcee) {
			String error = hcee.getResponseBodyAsString();
			throw new ElidomRuntimeException("RECEIVE_BATCH_ERROR", error, hcee);
			
		} catch(Exception e) {
			throw new ElidomRuntimeException("RECEIVE_BATCH_ERROR", e.getMessage(), e);
		}
	}
	
	/**
	 * 주문으로 Wave 구성
	 * 
	 * @param event
	 * @param waveSeq
	 * @param result
	 * @return
	 */
	@SuppressWarnings("unchecked")
	@Transactional(propagation=Propagation.REQUIRES_NEW)
	public Wave configureWaveByOrders(BatchReceiveEvent event, int waveSeq, Map<String, Object> result) {
		// 1. 조회 결과 파싱
		Map<String, Object> response = (Map<String, Object>)this.parseApiResult(result, null);
		
		// 2. 총 주문 수 파싱
		int totalOrders = ValueUtil.toInteger(response.get("release_cnt"));
		if(totalOrders == 0) {
			return null;
		}
		
		// 3. Wave 생성
		Wave wave = this.createNewWave(event, waveSeq);
		
		// 4. 수신 데이터로 주문 생성
		this.createOrdersByWave(event, wave, result);
		
		// 5. Wave 소속 주문의 주문 수, 상품 수, PCS 수를 모두 계산
		String sql = "select count(distinct(sku_cd)) as sku_qty, sum(order_qty) as total_pcs from orders where domain_id = :domainId and batch_id = :batchId";
		Map<String, Object> condition = ValueUtil.newMap("domainId,batchId", wave.getDomainId(), wave.getId());
		Wave orderQty = this.queryManager.selectBySql(sql, condition, Wave.class);
		wave.setOrderQty(totalOrders);
		wave.setSkuQty(orderQty.getSkuQty());
		wave.setTotalPcs(orderQty.getTotalPcs());
		this.queryManager.insert(wave);
		
		// 6. 커스텀 서비스 호출
		Map<String, Object> parameters = ValueUtil.newMap("event,wave", event, wave);
		this.customService.doCustomService(event.getDomainId(), DIY_HANDLE_BATCHES_BY_NOSNOS, parameters);
		
		// 7. 배치 리턴 
		return wave;
	}
	
	/**
	 * 새로운 배치 생성
	 * 
	 * @param event
	 * @param waveSeq
	 * @return
	 */
	private Wave createNewWave(BatchReceiveEvent event, int waveSeq) {
		String jobDate = event.getJobDate();
		String areaCd = event.getAreaCd();
		String waveId = areaCd + jobDate.replace(LogisConstants.DASH, LogisConstants.EMPTY_STRING) + LogisConstants.DASH + waveSeq;
		
		Wave wave = new Wave();
		wave.setDomainId(event.getDomainId());
		wave.setId(waveId);
		wave.setWaveNo(waveId);
		wave.setComCd(event.getComCd());
		wave.setJobDate(jobDate);
		wave.setJobSeq(ValueUtil.toString(waveSeq));
		wave.setStatus(JobBatch.STATUS_WAIT);
		return wave;
	}
	
	/**
	 * Wave로 주문 생성
	 * 
	 * @param event
	 * @param wave
	 * @param result
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void createOrdersByWave(BatchReceiveEvent event, Wave wave, Map<String, Object> result) {
		Map<String, Object> response = (Map<String, Object>)result.get("response");
		Page<Map> ordersPage = this.parseDataPage(response, "release_info", "current_page", "total_page", "release_cnt");
		
		// 응답 데이터 중에 총 페이지, 현재 페이지, 주문 전체 정보 추출
		int currentPage = ordersPage.getIndex();
		int totalPage = ordersPage.getSize();
		List<Map> orderList = ordersPage.getList();
		
		if(ValueUtil.isNotEmpty(orderList)) {
			// 주문 매핑 처리 - 커스텀 서비스 호출
			Map<String, Object> parameters = ValueUtil.newMap("wave,orders", wave, orderList);
			this.customService.doCustomService(wave.getDomainId(), DIY_HANDLE_ORDERS_BY_NOSNOS, parameters);
			
			// 현재 페이지가 마지막 페이지가 아니면 ...
			if(totalPage > currentPage) {
				result = this.receiveOrdersByJobSeq(wave.getJobDate(), ValueUtil.toInteger(wave.getJobSeq()), currentPage + 1);
				this.createOrdersByWave(event, wave, result);
			}
		}
	}
	
	/**
	 * NosNos API 조회 결과 파싱하기
	 * 
	 * @param result
	 * @param responseField
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map<String, ?> parseApiResult(Map<String, Object> result, String responseField) {
		// 1. status 체크
		if(result == null || !ValueUtil.toBoolean(result.get("status"))) {
			throw ThrowUtil.newValidationErrorWithNoLog("수신 오류가 발생했습니다.");
		}

		// 2. response 객체 리턴
		return (Map)result.get(ValueUtil.isEmpty(responseField) ? "response" : responseField);
	}
	
	/**
	 * 응답 데이터를 파싱하여 리스트로 리턴
	 * 
	 * @param response
	 * @param listField
	 * @param emptyErrorMsg
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private List<?> parseDateList(Map<String, Object> response, String listField, String emptyErrorMsg) {
		List<Map> dataList = (List<Map>)response.get(listField);
		
		if(ValueUtil.isEmpty(dataList)) {
			throw ThrowUtil.newValidationErrorWithNoLog(emptyErrorMsg);
		}
		
		return dataList;
	}
	
	/**
	 * 응답 데이터에서 총 레코드 수, 현재 페이지 수, 총 페이지 수, 데이터 리스트를 추출
	 * 
	 * @param response
	 * @param listField
	 * @param curPageField
	 * @param totalPageField
	 * @param totalCountField
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Page<Map> parseDataPage(Map<String, Object> response, String listField, String curPageField, String totalPageField, String totalCountField) {
		// 응답 데이터 중에 총 페이지, 현재 페이지, 주문 전체 정보 추출
		int currentPage = ValueUtil.toInteger(response.get(curPageField));
		int totalPage = ValueUtil.toInteger(response.get(totalPageField));
		int totalCount = ValueUtil.toInteger(response.get(totalCountField));
		List<Map> orderList = (List<Map>)response.get(listField);
		
		Page<Map> page = new Page<Map>();
		page.setSize(totalPage);
		page.setIndex(currentPage);
		page.setTotalSize(totalCount);
		page.setList(orderList);
		return page;
	}
	
	/*************************************************************************************************/
	/*												인 증 관 련 										 */
	/*************************************************************************************************/
	 
	/**
	 * 인증 헤더 구성
	 * 
	 * @param reqDate
	 * @return
	 */
	private HttpEntity<?> createAuthHeaders(String reqDate) {
		HttpHeaders headers = new HttpHeaders();
		headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
		headers.setCacheControl("no-cache");
		headers.add(HttpHeaders.AUTHORIZATION, "SRWMS-HMAC-SHA256");
		headers.add("Credential", this.getCredential(reqDate));
		headers.add("Signature", this.getSignature(reqDate));
		return new HttpEntity<>(headers);
	}
	
	/**
	 * credential 키
	 * 
	 * @param reqDate
	 * @return
	 */
	private String getCredential(String reqDate) {
		String companyCode = this.getCompanyCode();
		String apiAccessKey = this.getApiAccessKey();
		return companyCode + "/" + apiAccessKey + "/" + reqDate + "/srwms_request";
	}
	
	/**
	 * Signature 키
	 * 
	 * @param reqDate
	 * @return
	 */
	private String getSignature(String reqDate) {
		// 1. Datekey
		String dateKey = this.getSignatureDateKey(reqDate);
		
		// 2. Signkey
		String signKey = this.getSignatureSignKey(dateKey);
		
		// 3. Signature
		Encoder encoder = Base64.getEncoder(); 
		return encoder.encodeToString(signKey.getBytes());
	}
	
	/**
	 * Signature DateKey 리턴 : HMAC-SHA256(<api-secret-key> , <요청일자<YYYYMMDD>>)
	 * 
	 * @param reqDate
	 * @return
	 */
	private String getSignatureDateKey(String reqDate) {
		String apiSecretKey = this.getApiSecretKey();
		return this.toHmacSHA256(apiSecretKey, reqDate);
	}
	
	/**
	 * Signature DateKey 리턴 : HMAC-SHA256(Datekey , <api-access-key>)
	 * 
	 * @param dateKey
	 * @return
	 */
	private String getSignatureSignKey(String dateKey) {
		String apiAccessKey = this.getApiAccessKey();
		return this.toHmacSHA256(dateKey, apiAccessKey);
	}
	
	/**
	 * 기업 코드
	 * 
	 * @return
	 */
	private String getCompanyCode() {
		return SettingUtil.getValue("3rdparty.wms.nosnos.company", "X8956");
	}
	
	/**
	 * API Access Key
	 * 
	 * @return
	 */
	private String getApiAccessKey() {
		return SettingUtil.getValue("3rdparty.wms.nosnos.api-access", "7jHqyzFUiK6aBaor");
	}
	
	/**
	 * API Secret Key
	 * 
	 * @return
	 */
	private String getApiSecretKey() {
		return SettingUtil.getValue("3rdparty.wms.nosnos.api-secret", "6xILVouOLGRKnxGuiuqI");
	}
	
	/**
	 * HMAC-SHA256 암호화 처리
	 * 
	 * @param secretKey
	 * @param data
	 * @return
	 */
	private String toHmacSHA256(String secretKey, String data) {
		byte[] result = null;
		
		try {
			String algorithm = "HmacSHA256";
			Mac mac = Mac.getInstance(algorithm);
			mac.init(new SecretKeySpec(secretKey.getBytes(), algorithm));
			result = mac.doFinal(data.getBytes());
			
		} catch(Exception e) {
			throw new ElidomRuntimeException("HMAC_SHA256_ENCODING_ERROR", e);
		}
		
		return Hex.encodeHexString(result);
	}

}
