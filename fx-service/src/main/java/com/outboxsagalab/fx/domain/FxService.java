package com.outboxsagalab.fx.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.outboxsagalab.fx.idempotency.ProcessedEvent;
import com.outboxsagalab.fx.idempotency.ProcessedEventRepository;
import com.outboxsagalab.fx.messaging.Topics;
import com.outboxsagalab.fx.outbox.OutboxEntry;
import com.outboxsagalab.fx.outbox.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.UUID;

/**
 * Handles {@code ConvertCurrency} and {@code ReverseConversion} commands.
 * Same idempotent-saga-participant recipe as account-service.
 *
 * <p>Conversion is deterministic: read the seeded rate, multiply.
 * If no rate exists for the (base, quote) pair, emit ConversionRejected.
 */
@Service
public class FxService {

    private static final Logger log = LoggerFactory.getLogger(FxService.class);

    private final FxRateRepository rates;
    private final FxTradeRepository trades;
    private final OutboxRepository outbox;
    private final ProcessedEventRepository processedEvents;
    private final ObjectMapper json;

    public FxService(FxRateRepository rates,
                     FxTradeRepository trades,
                     OutboxRepository outbox,
                     ProcessedEventRepository processedEvents,
                     ObjectMapper json) {
        this.rates = rates;
        this.trades = trades;
        this.outbox = outbox;
        this.processedEvents = processedEvents;
        this.json = json;
    }

    @Transactional
    public void convert(UUID inboundEventId, UUID transferId,
                        String baseCurrency, String quoteCurrency, BigDecimal baseAmount) {
        if (alreadyProcessed(inboundEventId, "ConvertCurrency", transferId)) return;

        var rateRow = rates.findByBaseCurrencyAndQuoteCurrency(baseCurrency, quoteCurrency);
        if (rateRow.isEmpty()) {
            log.info("Conversion rejected (no rate) transfer_id={} {} -> {}",
                    transferId, baseCurrency, quoteCurrency);
            processedEvents.save(new ProcessedEvent(inboundEventId, "ConvertCurrency"));
            ObjectNode reject = json.createObjectNode();
            reject.put("transfer_id", transferId.toString());
            reject.put("reason", "RATE_UNAVAILABLE");
            enqueue(transferId, "ConversionRejected", reject);
            return;
        }

        BigDecimal rate = rateRow.get().getRate();
        BigDecimal quoteAmount = baseAmount.multiply(rate).setScale(4, RoundingMode.HALF_UP);

        FxTrade trade = new FxTrade(UUID.randomUUID(), baseCurrency, quoteCurrency,
                baseAmount, quoteAmount, rate, FxTradeStatus.EXECUTED, transferId);
        trades.save(trade);
        processedEvents.save(new ProcessedEvent(inboundEventId, "ConvertCurrency"));

        ObjectNode ok = json.createObjectNode();
        ok.put("transfer_id", transferId.toString());
        ok.put("base_currency", baseCurrency);
        ok.put("quote_currency", quoteCurrency);
        ok.put("base_amount", baseAmount);
        ok.put("quote_amount", quoteAmount);
        ok.put("rate", rate);
        enqueue(transferId, "CurrencyConverted", ok);

        log.info("Converted transfer_id={} {} {} -> {} {} (rate={})",
                transferId, baseAmount, baseCurrency, quoteAmount, quoteCurrency, rate);
    }

    @Transactional
    public void reverse(UUID inboundEventId, UUID transferId) {
        if (alreadyProcessed(inboundEventId, "ReverseConversion", transferId)) return;

        var executed = trades.findByCorrelationIdAndStatus(transferId, FxTradeStatus.EXECUTED);
        if (executed.isEmpty()) {
            // Defensive: nothing to reverse. Mark processed and emit FxReversed anyway
            // so the saga continues to the refund step.
            processedEvents.save(new ProcessedEvent(inboundEventId, "ReverseConversion"));
            ObjectNode noop = json.createObjectNode();
            noop.put("transfer_id", transferId.toString());
            noop.put("note", "no executed trade to reverse");
            enqueue(transferId, "FxReversed", noop);
            log.info("Reverse no-op (no executed trade) transfer_id={}", transferId);
            return;
        }

        FxTrade orig = executed.get(0);
        FxTrade reversal = new FxTrade(UUID.randomUUID(),
                orig.getBaseCurrency(), orig.getQuoteCurrency(),
                orig.getBaseAmount(), orig.getQuoteAmount(), orig.getRate(),
                FxTradeStatus.REVERSED, transferId);
        trades.save(reversal);
        processedEvents.save(new ProcessedEvent(inboundEventId, "ReverseConversion"));

        ObjectNode ok = json.createObjectNode();
        ok.put("transfer_id", transferId.toString());
        ok.put("reversed_trade_id", orig.getId().toString());
        enqueue(transferId, "FxReversed", ok);

        log.info("Reversed transfer_id={} original_trade={}", transferId, orig.getId());
    }

    private boolean alreadyProcessed(UUID eventId, String type, UUID sagaId) {
        if (processedEvents.existsById(eventId)) {
            log.info("Skipping already-processed {} event_id={} saga_id={}", type, eventId, sagaId);
            return true;
        }
        return false;
    }

    private void enqueue(UUID sagaId, String eventType, ObjectNode innerPayload) {
        UUID outboundEventId = UUID.randomUUID();
        ObjectNode envelope = json.createObjectNode();
        envelope.put("event_id", outboundEventId.toString());
        envelope.put("event_type", eventType);
        envelope.put("saga_id", sagaId.toString());
        envelope.put("occurred_at", Instant.now().toString());
        envelope.set("payload", innerPayload);

        String body;
        try {
            body = json.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize outbox envelope", e);
        }

        outbox.save(new OutboxEntry(outboundEventId, sagaId.toString(),
                Topics.FX_EVENTS, eventType, body));
        log.info("Enqueued outbox event saga_id={} event_type={} event_id={}",
                sagaId, eventType, outboundEventId);
    }
}
