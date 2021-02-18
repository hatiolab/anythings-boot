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
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import xyz.anythings.base.LogisConstants;
import xyz.anythings.base.entity.JobBatch;
import xyz.anythings.base.event.main.BatchReceiveEvent;
import xyz.anythings.sys.service.ICustomService;
import xyz.elidom.exception.server.ElidomRuntimeException;
import xyz.elidom.orm.IQueryManager;
import xyz.elidom.sys.SysConstants;
import xyz.elidom.sys.util.SettingUtil;
import xyz.elidom.sys.util.ThrowUtil;
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
	 * 주문 수신 서머리 커스텀 서비스
	 */
	private static final String DIY_RECEIVE_SUMMARY_BY_NOSNOS = "diy-receive-summary-by-nosnos";
	/**
	 * 주문 수신 커스텀 서비스
	 */
	private static final String DIY_RECEIVE_ORDERS_BY_NOSNOS = "diy-receive-orders-by-nosnos";
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
		
		// 2. 서머리 수신 결과 체크
		if(summary == null || !ValueUtil.toBoolean(summary.get("status"))) {
			throw ThrowUtil.newValidationErrorWithNoLog("수신할 정보가 없습니다.");
		}
		
		// 3. 수신할 회차 정보 추출
		Map<String, Object> response = (Map<String, Object>)summary.get("response");
		List<Map> batchSeqList = (List<Map>)response.get("shipping_info");
		
		if(ValueUtil.isEmpty(batchSeqList)) {
			throw ThrowUtil.newValidationErrorWithNoLog("수신할 회차 정보가 없습니다.");
		}
		
		List<String> jobSeqList = new ArrayList<String>();
		for(Map batchSeq : batchSeqList) {
			jobSeqList.add(ValueUtil.toString(batchSeq.get("seq")));
		}
		
		if(ValueUtil.isEmpty(jobSeqList)) {
			throw ThrowUtil.newValidationErrorWithNoLog("수신할 회차 정보가 없습니다.");
		}
		
		// 4. 이벤트에 수신할 회차 정보 결과 설정
		event.setResult(jobSeqList);
		
		// 5. 커스텀 서비스 호출
		Map<String, Object> parameters = ValueUtil.newMap("event", event);
		this.customService.doCustomService(event.getDomainId(), DIY_RECEIVE_SUMMARY_BY_NOSNOS, parameters);
		
		// 6. 주문 수신
		this.handleStartToReceive(event);
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
		Map<String, Object> params = ValueUtil.newMap("domainId,jobDate,jobSeqList,jobType,areaCd,stageCd,comCd", event.getDomainId(), event.getJobDate(), jobSeqList, event.getJobType(), event.getAreaCd(), event.getStageCd(), event.getComCd());
		String sql = "select job_seq from job_batches where domain_id = :domainId and job_date = :jobDate and job_seq in (:jobSeqList) #if($jobType) and job_type = :jobType #end #if($areaCd) and area_cd = :areaCd #end #if($stageCd) and stage_cd = :stageCd #end #if($comCd) and com_cd = :comCd #end";
		List<String> receivedJobSeqList = this.queryManager.selectListBySql(sql, params, String.class, 0, 0);
		
		// 3. 수신할 차수 필터링
		for(String jobSeq : receivedJobSeqList) {
			jobSeqList.remove(jobSeq);
		}
		
		if(ValueUtil.isEmpty(jobSeqList)) {
			throw ThrowUtil.newValidationErrorWithNoLog("수신할 회차 정보가 없습니다.");
		}
		
		// 4. 새로운 회차에 대해서 주문 수신 시작
		for(String jobSeq : jobSeqList) {
			Map<String, Object> response = this.receiveOrdersByJobSeq(event.getJobDate(), ValueUtil.toInteger(jobSeq), 1);
			this.configureBatchByOrders(event, response);
		}
		
		// 5. 커스텀 서비스 호출 
		Map<String, Object> parameters = ValueUtil.newMap("event", event);
		this.customService.doCustomService(event.getDomainId(), DIY_RECEIVE_ORDERS_BY_NOSNOS, parameters);
	}

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
	 * 주문으로 배치 구성
	 * 
	 * @param event
	 * @param response
	 * @return
	 */
	private JobBatch configureBatchByOrders(BatchReceiveEvent event, Map<String, Object> response) {
		// 1. 주문 체크
		if(response == null || !ValueUtil.toBoolean(response.get("status"))) {
			throw ThrowUtil.newValidationErrorWithNoLog("수신할 주문 정보가 없습니다.");
		}
		
		// 2. 작업 배치 생성
		JobBatch batch = this.createNewBatch(event);
		
		// 3. 수신 데이터로 주문 생성
		this.createOrdersByBatch(batch, response);
		
		// 4. 배치 생성
		this.queryManager.insert(batch);
		
		// 5. 배치 리턴 
		return batch;
	}
	
	/**
	 * 새로운 배치 생성
	 * 
	 * @param event
	 * @return
	 */
	private JobBatch createNewBatch(BatchReceiveEvent event) {
		String jobDate = event.getJobDate();
		String jobSeq = event.getJobSeq();
		String batchId = event.getJobDate().replace(LogisConstants.DASH, LogisConstants.EMPTY_STRING) + LogisConstants.DASH + jobSeq;
		String areaCd = event.getAreaCd();
		String stageCd = event.getStageCd();
		String comCd = event.getComCd();
		
		JobBatch batch = new JobBatch();
		batch.setId(batchId);
		batch.setAreaCd(areaCd);
		batch.setStageCd(stageCd);
		batch.setComCd(comCd);
		batch.setJobDate(jobDate);
		batch.setJobSeq(jobSeq);
		return batch;
	}
	
	/**
	 * 작업 배치로 주문 생성
	 * 
	 * @param batch
	 * @param response
	 */
	private void createOrdersByBatch(JobBatch batch, Map<String, Object> response) {
		response.get("re");
	}
	
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
