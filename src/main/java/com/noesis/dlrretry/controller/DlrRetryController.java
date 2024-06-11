package com.noesis.dlrretry.controller;

import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.noesis.dlrretry.kafka.DlrRetryForwarder;
import com.noesis.domain.persistence.NgDlrFailed;
import com.noesis.domain.persistence.NgDlrMessage;
import com.noesis.domain.persistence.NgUser;
import com.noesis.domain.platform.DlrObject;
import com.noesis.domain.platform.DlrRetryMessageObject;
import com.noesis.domain.service.DlrMisService;
import com.noesis.domain.service.FailedDlrMessageService;
import com.noesis.domain.service.UserService;


@RestController
public class DlrRetryController {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Value("${kafka.topic.push.dlr.high.priority}")
	private String pushDlrTopicName;
	
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
	
	@RequestMapping(value = "/retryspecificdlr/{messageId}", method = RequestMethod.GET)
	public NgDlrMessage retrySpecificDlr(@PathVariable String messageId) {
		logger.info("Request received for specific dlr retry with message Id : {}",messageId);
		NgDlrMessage ngDlrMessage = null;
		try {
			// Update retry status in failed table.
			NgDlrFailed failedDlrObject = failedDlrMessageService.getFailedDlrMessageIdAndRetriedStatus(messageId, 'N');
			int retryCount = failedDlrObject.getRetryCount();
			retryCount = retryCount+1;
			logger.info("New Dlr Retry Count is : "+ retryCount); 
			failedDlrObject.setRetryCount(retryCount);
			failedDlrObject.setIsRetried('Y');
			failedDlrObject.setRetryTs(new Timestamp(System.currentTimeMillis()));
			failedDlrMessageService.saveFailedDlrMessageObject(failedDlrObject);
			logger.info("Retry Status Changed for message id: "+failedDlrObject.getMessageId());
			
			ngDlrMessage = dlrMisService.getDlrMisMessageFromMessageId(messageId);
			NgUser ngUser  =userService.getUserById(ngDlrMessage.getUserId());
			DlrObject dlrObject = convertDlrMisObjectIntoDlrObject(ngDlrMessage, ngUser);
			
			if(ngUser.getNgDlrMediumType().getCode().equalsIgnoreCase("push")) {
				String dlrObjectMessageAsString = objectMapper.writeValueAsString(dlrObject);
				dlrRetryForwarder.send(pushDlrTopicName, dlrObjectMessageAsString);
			}else {
				String dlrObjectMessageAsString = objectMapper.writeValueAsString(dlrObject);
				dlrRetryForwarder.send(dlrRetryTopicName, dlrObjectMessageAsString);
			}
			
			
		} catch (JsonProcessingException e) {
			logger.error("Exception occured while retring dlr message {}", messageId);
			e.printStackTrace();
		}
		
		return ngDlrMessage;
	}
	
	@RequestMapping(value = "/retryuserdlr/{userId}", method = RequestMethod.GET)
	public String retryUserDlr(@PathVariable String userId) {
		logger.info("Request received for user wise dlr retry with user Id : {}",userId);
		
		DlrRetryMessageObject messageObject = new DlrRetryMessageObject();
		messageObject.setUserWiseRetry(true);
		messageObject.setUserId(userId);
		messageObject.setAllDlrRetry(false);
		String dlrRetryMessageObjectAsString;
		try {
			dlrRetryMessageObjectAsString = objectMapper.writeValueAsString(messageObject);
			dlrRetryForwarder.send(dlrRetryTaskTopicName, dlrRetryMessageObjectAsString);
			
		} catch (JsonProcessingException e1) {
			logger.error("Error while accepting dlr retry request.");
			e1.printStackTrace();
			return "Request Failed";
		}
		return "Request Accepted.";
		
	} 
	
	@RequestMapping(value = "/retryallfaileddlr", method = RequestMethod.GET)
	public String retryAllFailedDlr() {
		logger.info("Request received for All DLR retry.");
		
		DlrRetryMessageObject messageObject = new DlrRetryMessageObject();
		messageObject.setUserWiseRetry(false);
		messageObject.setAllDlrRetry(true);
		String dlrRetryMessageObjectAsString;
		try {
			dlrRetryMessageObjectAsString = objectMapper.writeValueAsString(messageObject);
			dlrRetryForwarder.send(dlrRetryTaskTopicName, dlrRetryMessageObjectAsString);
			
		} catch (JsonProcessingException e1) {
			logger.error("Error while accepting dlr retry request.");
			e1.printStackTrace();
			return "Request Failed";
		}
		return "Request Accepted.";
		
	}
	
	private DlrObject convertDlrMisObjectIntoDlrObject(NgDlrMessage ngDlrMessage, NgUser ngUser) {
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
		
		if(ngUser.getId() == 707 || ngUser.getId() == 712) {
			dlrObject.setCustRef("1");
		}
		
		if(ngUser.getId() == 707 || ngUser.getId() == 712) {
			dlrObject.setReqId("1");
		}
		return dlrObject;
	}
}
