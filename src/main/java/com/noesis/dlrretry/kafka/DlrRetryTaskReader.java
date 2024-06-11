package com.noesis.dlrretry.kafka;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.noesis.domain.persistence.NgDlrFailed;
import com.noesis.domain.persistence.NgDlrMessage;
import com.noesis.domain.persistence.NgDlrRetryTmp;
import com.noesis.domain.platform.DlrObject;
import com.noesis.domain.platform.DlrRetryMessageObject;
import com.noesis.domain.service.DlrMisService;
import com.noesis.domain.service.FailedDlrMessageService;
import com.noesis.domain.service.UserService;

 
public class DlrRetryTaskReader{

	private static final Logger logger = LogManager.getLogger(DlrRetryTaskReader.class);

	private int maxPollRecordSize;
	
	private CountDownLatch latch = new CountDownLatch(maxPollRecordSize);
	
	public CountDownLatch getLatch() {
		return latch;
	}
	
	@Value("${app.name}")
	private String appName;
	
	@Value("${kafka.dlrreader.sleep.interval.ms}")
	private String dlrReaderSleepInterval;
	
	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private DlrMisService dlrMisService;
	
	@Autowired
	private FailedDlrMessageService failedDlrMessageService;
	
	@Autowired
	private UserService userService;
	 
	@Autowired
	private DlrRetryForwarder dlrRetryForwarder;
	
	@Value("${kafka.topic.name.dlr.retry}")
	private String dlrRetryTopicName;
	
	@Value("${kafka.topic.name.dlr.retry.task}")
	private String dlrRetryTaskTopicName;
	
	@Value("${kafka.dlr.retry.consumer.sleep.interval.ms}")
	private String consumerSleepTime;
	
	@Value("${failed.dlr.page.size}")
	private String failedDlrPageSize; 
	
	@Value("${smpp.instance.ids}")
    private String smppInstanceIds;
    
	private HashMap<String, String> smppInstanceDlrTopicMap;
	
	public DlrRetryTaskReader(int maxPollRecordSize) {
		this.maxPollRecordSize = maxPollRecordSize;
		this.smppInstanceDlrTopicMap = new HashMap<String, String>();
	}
	
	 @PostConstruct
	    public void generateSmppTopicMap() {
	        final String[] smppInstanceIdList = this.smppInstanceIds.split(",");
	        final String[] smppDlrTopicList = this.dlrRetryTopicName.split(",");
	        for (int i = 0; i < smppInstanceIdList.length; i++) {
	            this.smppInstanceDlrTopicMap.put(smppInstanceIdList[i], smppDlrTopicList[i]);
	        }
	        DlrRetryTaskReader.logger.info("SmppInstanceTopicMap : {}", (Object)this.smppInstanceDlrTopicMap.toString());
	    }
    
	
	@KafkaListener(id = "${kafka.topic.name.dlr.retry.task}", topics = "${kafka.topic.name.dlr.retry.task}",
			groupId = "${kafka.topic.name.dlr.retry.task}",
			idIsGroup = false)
	  public void receive(List<String> messageList,
	      @Header(KafkaHeaders.RECEIVED_PARTITION_ID) List<Integer> partitions,
	      @Header(KafkaHeaders.OFFSET) List<Long> offsets) {

	    logger.info("Request received for DLR Retry Task. "+messageList.size());
		for (int i = 0; i < messageList.size(); i++) {
			DlrRetryMessageObject dlrRetryTaskMessageObject = convertMessageIntoDlrRetryMessageObject(messageList.get(i));
			if(dlrRetryTaskMessageObject.isUserWiseRetry()) {
				processUserWiseDlrRetryRequest(dlrRetryTaskMessageObject.getUserId());
			}else if(dlrRetryTaskMessageObject.isAllDlrRetry()) {
				processAllDlrRetryRequest();
			}
			latch.countDown();
		}
	    logger.info("End of received DLR batch.");
	    try {
	    	logger.info("DlrReader Thread Going To Sleep for "+dlrReaderSleepInterval + "ms.");
	    	Thread.sleep(Integer.parseInt(dlrReaderSleepInterval));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	  }
	
	
	private void processAllDlrRetryRequest() {
		int dlrCountToBeProcessed = 0;
		int pageSize = Integer.parseInt(failedDlrPageSize);
		int sleepTime = Integer.parseInt(consumerSleepTime);
		try {
			do {
				dlrCountToBeProcessed = findAndProcessAllDlr(pageSize);
				Thread.sleep(sleepTime);
			}while(dlrCountToBeProcessed>0);	
		} catch (JsonProcessingException e) {
			logger.error("Exception occured while retring all dlr message.");
			e.printStackTrace();
		} catch (InterruptedException e) {
			logger.error("Exception occured while retring all dlr message.");
			e.printStackTrace();
		}catch (Exception e) {
			logger.error("Exception occured while retring all dlr message.");
			e.printStackTrace();
		}
	}

	//@Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRES_NEW)
	private int findAndProcessAllDlr(int pageSize) throws JsonProcessingException {
		int dlrCountToBeProcess = 0;
		try {
			
			NgDlrMessage ngDlrMessage;
			List<NgDlrFailed> failedDlrObjectList;
			logger.info("Going to fetch next 5 failed DLR from DB for retry");
			failedDlrObjectList = failedDlrMessageService.getFailedDlrList('N', pageSize);
			dlrCountToBeProcess = failedDlrObjectList.size();
			if(failedDlrObjectList != null && failedDlrObjectList.size()>0) {
				for (NgDlrFailed ngDlrFailedObject : failedDlrObjectList) {
					int retryCount = ngDlrFailedObject.getRetryCount();
					retryCount = retryCount+1;
					ngDlrFailedObject.setRetryCount(retryCount);
					ngDlrFailedObject.setIsRetried('Y');
					ngDlrFailedObject.setRetryTs(new Timestamp(System.currentTimeMillis()));
				}
				failedDlrMessageService.saveFailedDlrList(failedDlrObjectList);
				for (NgDlrFailed ngDlrFailedObject : failedDlrObjectList) {
					ngDlrMessage = dlrMisService.getDlrMisMessageFromMessageId(ngDlrFailedObject.getMessageId());
					DlrObject dlrObject = convertDlrMisObjectIntoDlrObject(ngDlrMessage);
					
					String dlrObjectMessageAsString = objectMapper.writeValueAsString(dlrObject);
					//dlrRetryForwarder.send(dlrRetryTopicName, dlrObjectMessageAsString);
					dlrRetryForwarder.send((String)this.smppInstanceDlrTopicMap.get(dlrObject.getSystemId()), dlrObjectMessageAsString);
				}
			}
		}catch(Exception e) {
			logger.error("Error in method findAndProcessAllDlr. Hence stopping the processing till next request.");
			e.printStackTrace();
		}
		return dlrCountToBeProcess;
	}

	private void processUserWiseDlrRetryRequest(String userId) {
		int dlrCountToBeProcessed = 0;
		int pageSize = Integer.parseInt(failedDlrPageSize);
		int sleepTime = Integer.parseInt(consumerSleepTime);
		try {
			do {
				dlrCountToBeProcessed = findAndProcessDlrForUser(userId, pageSize);
				Thread.sleep(sleepTime);
			}while(dlrCountToBeProcessed > 0);	
		} catch (InterruptedException e) {
			logger.error("Exception occured while retring dlr message for user id {}", userId);
			e.printStackTrace();
		} catch (Exception e) {
			logger.error("Exception occured while retring dlr message for user id {}", userId);
			e.printStackTrace();
		}
	}

	//@Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRES_NEW)
	private int findAndProcessDlrForUser(String userId, int pageSize) {
		int dlrCountToBeProcessed = 0;
		try {
			//NgDlrMessage ngDlrMessage;
			NgDlrRetryTmp ngDlrRetryTmpMessage;
			List<NgDlrFailed> failedDlrObjectList;
			logger.info("Going to fetch next 5 failed DLR from DB for retry for user {}",userId);
			failedDlrObjectList = failedDlrMessageService.getFailedDlrListByUserId(Integer.parseInt(userId) , 'N', pageSize);
			dlrCountToBeProcessed = failedDlrObjectList.size();
			if(failedDlrObjectList != null && failedDlrObjectList.size()>0) {
				for (NgDlrFailed ngDlrFailedObject : failedDlrObjectList) {
					int retryCount = ngDlrFailedObject.getRetryCount();
					retryCount = retryCount+1;
					ngDlrFailedObject.setRetryCount(retryCount);
					ngDlrFailedObject.setIsRetried('Y');
					ngDlrFailedObject.setRetryTs(new Timestamp(System.currentTimeMillis()));
				}
				failedDlrMessageService.saveFailedDlrList(failedDlrObjectList);
				for (NgDlrFailed ngDlrFailedObject : failedDlrObjectList) {
					try {
						ngDlrRetryTmpMessage = dlrMisService.getDlrMisMessageFromTmpMessageId(ngDlrFailedObject.getMessageId());
						if(ngDlrRetryTmpMessage!=null) {
							DlrObject dlrObject = convertDlrMisObjectIntoDlrObject(ngDlrRetryTmpMessage);
							String dlrObjectMessageAsString = objectMapper.writeValueAsString(dlrObject);
							//dlrRetryForwarder.send(dlrRetryTopicName, dlrObjectMessageAsString);
							dlrRetryForwarder.send((String)this.smppInstanceDlrTopicMap.get(dlrObject.getSystemId()), dlrObjectMessageAsString);
						}else {
							logger.info("No DLR found in mis table for message id  : {}",ngDlrFailedObject.getMessageId());
						}
					}catch(Exception e) {
						e.printStackTrace();
						logger.info("Error occured while retrying dlr for message id: {} . Hence skipping the DLR.",ngDlrFailedObject.getMessageId());
					}
				}
			}
			return dlrCountToBeProcessed;
		}catch(Exception e) {
			logger.error("Error in method findAndProcessDlrForUser. Hence stopping the processing till next request.");
			e.printStackTrace();
			return 0;
		}
	}

	private DlrObject convertDlrMisObjectIntoDlrObject(NgDlrMessage ngDlrMessage) {
		DlrObject dlrObject = new DlrObject();
		dlrObject.setCarrierId(""+ngDlrMessage.getCarrierId());
		dlrObject.setCircleId(""+ngDlrMessage.getCircleId());
		dlrObject.setDestNumber(ngDlrMessage.getMobileNumber());
		dlrObject.setDlrStatusCode(ngDlrMessage.getStatusId());
		dlrObject.setDlrType(ngDlrMessage.getDlrType());
		dlrObject.setDr(ngDlrMessage.getDlrReceipt());
		dlrObject.setErrorCode(ngDlrMessage.getErrorCode());
		dlrObject.setErrorDesc(ngDlrMessage.getErrorDesc());
		dlrObject.setExpiry(ngDlrMessage.getExpiry());
		dlrObject.setMessageId(ngDlrMessage.getMessageId());
		dlrObject.setReceiveTime(new Timestamp(ngDlrMessage.getReceivedTs().getTime()));
		dlrObject.setRouteId(""+ngDlrMessage.getRouteId());
		dlrObject.setSenderId(ngDlrMessage.getSenderId());
		dlrObject.setSmscid(ngDlrMessage.getSmscId());
		dlrObject.setSubmitTime(new Timestamp(ngDlrMessage.getDeliveredTs().getTime()));
		dlrObject.setSystemId(ngDlrMessage.getMessageSource());
		dlrObject.setUserName(userService.getUserById(ngDlrMessage.getUserId()).getUserName());
		return dlrObject;
	}
	
	private DlrObject convertDlrMisObjectIntoDlrObject(NgDlrRetryTmp ngDlrMessage) {
		DlrObject dlrObject = new DlrObject();
		dlrObject.setCarrierId(""+ngDlrMessage.getCarrierId());
		dlrObject.setCircleId(""+ngDlrMessage.getCircleId());
		dlrObject.setDestNumber(ngDlrMessage.getMobileNumber());
		dlrObject.setDlrStatusCode(ngDlrMessage.getStatusId());
		dlrObject.setDlrType(ngDlrMessage.getDlrType());
		dlrObject.setDr(ngDlrMessage.getDlrReceipt());
		dlrObject.setErrorCode(ngDlrMessage.getErrorCode());
		dlrObject.setErrorDesc(ngDlrMessage.getErrorDesc());
		dlrObject.setExpiry(ngDlrMessage.getExpiry());
		dlrObject.setMessageId(ngDlrMessage.getMessageId());
		dlrObject.setReceiveTime(new Timestamp(ngDlrMessage.getReceivedTs().getTime()));
		dlrObject.setRouteId(""+ngDlrMessage.getRouteId());
		dlrObject.setSenderId(ngDlrMessage.getSenderId());
		dlrObject.setSmscid(ngDlrMessage.getSmscId());
		dlrObject.setSubmitTime(new Timestamp(ngDlrMessage.getDeliveredTs().getTime()));
		dlrObject.setSystemId(ngDlrMessage.getMessageSource());
		dlrObject.setUserName(userService.getUserById(ngDlrMessage.getUserId()).getUserName());
		return dlrObject;
	}
	
	private DlrRetryMessageObject convertMessageIntoDlrRetryMessageObject(String dlrRetryMessage) {
		DlrRetryMessageObject dlrRetryMessageObject = null;
		try {
			dlrRetryMessageObject = objectMapper.readValue(dlrRetryMessage, DlrRetryMessageObject.class);
		} catch (Exception e){
			logger.error("Dont retry this message as error while parsing DLR Retry Task Request json string: "+dlrRetryMessage);
			e.printStackTrace();
		} 
		return dlrRetryMessageObject;
	}
}