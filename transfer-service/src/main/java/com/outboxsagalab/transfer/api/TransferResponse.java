package com.outboxsagalab.transfer.api;

import com.outboxsagalab.transfer.domain.Transfer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record TransferResponse(
        UUID id,
        String senderUser,
        String senderCurrency,
        String recipientUser,
        String recipientCurrency,
        BigDecimal sourceAmount,
        BigDecimal targetAmount,
        BigDecimal rate,
        String state,
        Instant createdAt,
        Instant updatedAt
) {
    public static TransferResponse from(Transfer t) {
        return new TransferResponse(
                t.getId(),
                t.getSenderUser(), t.getSenderCurrency(),
                t.getRecipientUser(), t.getRecipientCurrency(),
                t.getSourceAmount(), t.getTargetAmount(), t.getRate(),
                t.getState().name(),
                t.getCreatedAt(), t.getUpdatedAt());
    }
}
