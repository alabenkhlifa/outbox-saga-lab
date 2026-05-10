package com.outboxsagalab.transfer.api;

import java.math.BigDecimal;

public record CreateTransferRequest(
        String senderUser,
        String senderCurrency,
        String recipientUser,
        String recipientCurrency,
        BigDecimal sourceAmount
) { }
