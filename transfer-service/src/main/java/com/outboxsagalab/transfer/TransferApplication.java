package com.outboxsagalab.transfer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TransferApplication {
    public static void main(String[] args) {
        SpringApplication.run(TransferApplication.class, args);
    }
}
