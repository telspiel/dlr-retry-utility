package com.noesis.dlrretry;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@ComponentScan (basePackages = "com.noesis")
@RestController
@SpringBootApplication
@EnableJpaRepositories (basePackages = "com.noesis")
@EntityScan (basePackages = "com.noesis")
@EnableTransactionManagement
public class App extends SpringBootServletInitializer {

	@RequestMapping("/")
    String home()
    {
        return "Hello World - Consumer";
    }
	
	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
		return application.sources(App.class);
	}

	public static void main(String[] args) {
		SpringApplication.run(App.class, args);
	}
}
