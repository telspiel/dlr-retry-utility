package com.noesis.dlrretry.service;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.noesis.dlrretry.kafka.DlrRetryForwarder;
import com.noesis.domain.persistence.NgDlrUndeliveredRetry;
import com.noesis.domain.platform.DlrObject;
import com.noesis.domain.service.DlrMisRetryService;
import com.noesis.domain.service.UserService;

@Component
public class UndeliveredDlrRetryReader {

	private static final Logger logger = LogManager.getLogger(UndeliveredDlrRetryReader.class);

	private int maxPollRecordSize;

	private Boolean isRunning;

	private CountDownLatch latch = new CountDownLatch(maxPollRecordSize);

	public UndeliveredDlrRetryReader() {
	}

	public UndeliveredDlrRetryReader(int maxPollRecordSize) {
		this.maxPollRecordSize = maxPollRecordSize;
	}

	public CountDownLatch getLatch() {
		return latch;
	}

	@Value("${app.name}")
	private String appName;

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private DlrMisRetryService dlrMisRetryService;

	@Autowired
	private UserService userService;

	@Autowired
	private DlrRetryForwarder dlrRetryForwarder;
	
	@Value("${kafka.dlrreader.sleep.interval.ms}")
	private String dlrReaderSleepInterval;

	@Value("${kafka.topic.name.dlr.retry}")
	private String dlrRetryTopicName;

	@Value("${kafka.topic.name.dlr.retry.task}")
	private String dlrRetryTaskTopicName;

	@Value("${kafka.dlr.retry.consumer.sleep.interval.ms}")
	private String consumerSleepTime;

	@Value("${dlr.retry.max.count}")
	private String retryCount;

	@Value("${failed.dlr.page.size}")
	private String failedDlrPage;

	@Value("${dlr.retry.reader.sleep.interval.ms}")
	private String sleepTime;

	public void processAllFailedDlrRetryRequest() {
		Date toDate = null;
		Timestamp ts = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		logger.info("flag value before process in UndeliveredDlrRetryReader class :" + isRunning);
		try {
			while (isRunning) {
				toDate = sdf.parse(sdf.format(new Date()));
				ts = new Timestamp(toDate.getTime());
				findAndProcessAllDlr(ts);
			}
		} catch (

		JsonProcessingException e) {
			logger.error("Exception occured while retring all Failed dlr message.");
			e.printStackTrace();
		} catch (Exception e) {
			logger.error("Exception occured while retring all Failed dlr message.");
			e.printStackTrace();
		}
	}

	// @Transactional(isolation = Isolation.READ_COMMITTED, propagation =
	// Propagation.REQUIRES_NEW)
	public int findAndProcessAllDlr(Timestamp toDate) throws JsonProcessingException {
		int dlrCountToBeProcess = 0;
		try {

			List<NgDlrUndeliveredRetry> undeliveredDlrObjectList = dlrMisRetryService.getUndeliveredDlrList('N', 'N',
					Integer.parseInt(retryCount), "smpp", toDate);

			dlrCountToBeProcess = undeliveredDlrObjectList.size();

			if (undeliveredDlrObjectList != null && undeliveredDlrObjectList.size() > 0) {
				for (NgDlrUndeliveredRetry ngDlrFailedObject : undeliveredDlrObjectList) {

					// check for retry count from ng_user and new table and other wise delete
					if (userService.getUserById(ngDlrFailedObject.getUserId())
							.getRetryUndeliveredDlrCount() > ngDlrFailedObject.getRetryCount()) {

						logger.info("Check for retry count from ng_user and new table ng_dlr_undelivered_retry exist");

						int retryCount = ngDlrFailedObject.getRetryCount();
						retryCount = retryCount + 1;
						ngDlrFailedObject.setRetryCount(retryCount);
						ngDlrFailedObject.setRetryStatus('P');
						ngDlrFailedObject.setRetryTimeStamp(new Timestamp(System.currentTimeMillis()));

						// sending DLR again for retry
						DlrObject dlrObject = convertDlrMisObjectIntoDlrObject(ngDlrFailedObject);
						String dlrObjectMessageAsString = objectMapper.writeValueAsString(dlrObject);
						dlrRetryForwarder.send(dlrRetryTopicName, dlrObjectMessageAsString);

						// updating status in progress and retry count
						dlrMisRetryService.updateDlrStatusAndCount(ngDlrFailedObject);

					} else {
						// delete from NgDlrUndeliveredRetry after max retry

						logger.info(
								"ng_user and new table ng_dlr_undelivered_retry not exist, so delete from NgDlrUndeliveredRetry after max retry");

						dlrMisRetryService.deliveredRetryDataRemove(ngDlrFailedObject.getMessageId());
					}
				}
			} else {
				logger.info("No data found in ng_dlr_undelivered_retry, Thread sleep.");
				int t = Integer.parseInt(sleepTime);
				Thread.sleep(t);
			}
		} catch (InterruptedException e) {
			logger.error("Exception occured while retring all Failed dlr message.");
			e.printStackTrace();
		} catch (Exception e) {
			logger.error("Error in method findAndProcessAllDlr. Hence stopping the processing till next request.");
			e.printStackTrace();
		}
		return dlrCountToBeProcess;
	}

	private DlrObject convertDlrMisObjectIntoDlrObject(NgDlrUndeliveredRetry ngDlrMessage) {
		DlrObject dlrObject = new DlrObject();
		dlrObject.setCarrierId("" + ngDlrMessage.getCarrierId());
		dlrObject.setCircleId("" + ngDlrMessage.getCircleId());
		dlrObject.setDestNumber(ngDlrMessage.getMobileNumber());
		dlrObject.setDlrStatusCode(ngDlrMessage.getStatusId());
		dlrObject.setDlrType(ngDlrMessage.getDlrType());
		dlrObject.setDr(ngDlrMessage.getDlrReceipt());
		dlrObject.setErrorCode(ngDlrMessage.getErrorCode());
		dlrObject.setErrorDesc(ngDlrMessage.getErrorDesc());
		dlrObject.setExpiry(ngDlrMessage.getExpiry());
		dlrObject.setMessageId(ngDlrMessage.getMessageId());
		dlrObject.setReceiveTime(new Timestamp(ngDlrMessage.getReceivedTs().getTime()));
		dlrObject.setRouteId("" + ngDlrMessage.getRouteId());
		dlrObject.setSenderId(ngDlrMessage.getSenderId());
		dlrObject.setSmscid(ngDlrMessage.getSmscId());
		dlrObject.setSubmitTime(new Timestamp(ngDlrMessage.getDeliveredTs().getTime()));
		dlrObject.setSystemId(ngDlrMessage.getMessageSource());
		dlrObject.setUserName(userService.getUserById(ngDlrMessage.getUserId()).getUserName());
		return dlrObject;
	}

	public Boolean getIsRunning() {
		return isRunning;
	}

	public void setIsRunning(Boolean isRunning) {
		this.isRunning = isRunning;
	}

}
