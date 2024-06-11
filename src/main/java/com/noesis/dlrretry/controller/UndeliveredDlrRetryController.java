package com.noesis.dlrretry.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.noesis.dlrretry.service.UndeliveredDlrRetryReader;
import com.noesis.dlrretry.service.UndeliveredDlrRetryStatusUitility;

@RestController
public class UndeliveredDlrRetryController {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	UndeliveredDlrRetryReader undeliveredDlrRetryReader;

	@Autowired
	UndeliveredDlrRetryStatusUitility undeliveredDlrRetryStatusUitility;

	@RequestMapping(value = "/retryundelivereddlr/{isRunning}", method = RequestMethod.GET)
	public String retrySpecificDlr(@PathVariable Boolean isRunning) {
		logger.info("Request received for retrySpecificDlr dlr retry with Boolean flag value: {}", isRunning);
		String dlrRetryMessage = null;
		try {

			undeliveredDlrRetryReader.setIsRunning(isRunning);
			logger.info("Before calling dlr retry with Boolean flag value: {}" + isRunning);
			undeliveredDlrRetryReader.processAllFailedDlrRetryRequest();

			dlrRetryMessage = "sucessfully run undelivereddlr retring dlr flag value: " + isRunning;

		} catch (Exception e) {
			dlrRetryMessage = "Exception occured while retring retrySpecificDlr dlr flag value: " + isRunning;
			logger.error("Exception occured while retring undelivered dlr flag value: {}" + isRunning);
			e.printStackTrace();
		}

		return dlrRetryMessage;
	}

	@RequestMapping(value = "/retryundelivereddlrstatus/{isRunning}", method = RequestMethod.GET)
	public String retrySpecificDlrStatus(@PathVariable Boolean isRunning) {
		logger.info("Request received for retrySpecificDlrStatus() dlr retry with Boolean flag value: {}", isRunning);
		String dlrRetryMessage = null;
		try {

			undeliveredDlrRetryStatusUitility.setIsRunning(isRunning);
			logger.info("Before calling dlr status retry with Boolean flag value: {}");
			undeliveredDlrRetryStatusUitility.processDlrRetryUpdateRequest();

			dlrRetryMessage = "sucessfully run undelivereddlr retring dlr flag value: " + isRunning;

		} catch (Exception e) {
			dlrRetryMessage = "Exception occured while retring undelivered dlr flag value: " + isRunning;
			logger.error("Exception occured while retring undelivered dlr flag value: {}");
			e.printStackTrace();
		}

		return dlrRetryMessage;
	}

}
