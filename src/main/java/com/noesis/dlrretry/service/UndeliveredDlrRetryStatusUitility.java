package com.noesis.dlrretry.service;

import java.sql.Timestamp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.noesis.domain.service.DlrMisRetryService;

@Component
public class UndeliveredDlrRetryStatusUitility {

	private static final Logger logger = LogManager.getLogger(UndeliveredDlrRetryStatusUitility.class);

	private Boolean isRunning;

	public UndeliveredDlrRetryStatusUitility() {
		System.out.println("Default constructor UndeliveredDlrRetryStatusUitility :");
	}

	@Autowired
	private DlrMisRetryService dlrMisRetryService;

	@Value("${dlr.retry.max.count}")
	private String retryCount;

	@Value("${dlr.retry.statusuility.sleep.interval.ms}")
	private String sleepTime;

	public void processDlrRetryUpdateRequest() throws JsonProcessingException {

		logger.info("flag value before process in UndeliveredDlrRetryStatusUitility class :" + isRunning);

		while (isRunning) {

			dlrMisRetryService.retryDlrStatusUpdate(Integer.parseInt(retryCount),
					new Timestamp(System.currentTimeMillis()));
			try {
				int t = Integer.parseInt(sleepTime);
				Thread.sleep(t);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	public Boolean getIsRunning() {
		return isRunning;
	}

	public void setIsRunning(Boolean isRunning) {
		this.isRunning = isRunning;
	}

}
