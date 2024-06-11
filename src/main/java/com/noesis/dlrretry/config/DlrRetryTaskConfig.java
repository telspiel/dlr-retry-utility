package com.noesis.dlrretry.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.noesis.dlrretry.kafka.DlrRetryTaskReader;

@Configuration
@EnableKafka
public class DlrRetryTaskConfig {

  @Value("${kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${app.name}")
  private String appName;
  
  @Value("${kafka.auto.offset.reset.config}")
  private String kafkaAutoOffsetResetConfig;
  
  @Bean
  public Map<String, Object> consumerConfigs() {
    Map<String, Object> props = new HashMap<String, Object>();
    // list of host:port pairs used for establishing the initial connections to the Kafka cluster
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaAutoOffsetResetConfig); 
      
    return props;
  }

  @Bean
  public ConsumerFactory<String, String> consumerFactory() {
	  return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(),
		        new StringDeserializer());
  }

  @Bean
  public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<String, String>();
    factory.setConsumerFactory(consumerFactory());
    factory.setBatchListener(true);
    return factory;
  }

  @Bean
  public DlrRetryTaskReader dlrReaderTaskReader() {
    return new DlrRetryTaskReader(1);
  }
  
  @Bean
  public ObjectMapper objectMapper() {
	  return new ObjectMapper();
  }
}