package com.wfu.kafkahbasetest;

import com.wfu.kafkahbasetest.bean.Sender;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;

@SpringBootApplication
@EnableScheduling
public class KafkahbasetestApplication {

	@Autowired
	private Sender sender;

	public static void main(String[] args) {

		SpringApplication.run(KafkahbasetestApplication.class, args);

	}

	//然后每隔1秒执行一次
	@Scheduled(fixedRate = 1000)
	public void testKafka() throws Exception {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sender.send("my-test-topic", df.format(new Date()));
		System.out.println("生产成功...");
	}
}
