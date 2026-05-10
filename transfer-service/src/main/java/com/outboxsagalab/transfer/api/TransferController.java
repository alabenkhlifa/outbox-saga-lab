package com.outboxsagalab.transfer.api;

import com.outboxsagalab.transfer.domain.Transfer;
import com.outboxsagalab.transfer.domain.TransferRepository;
import com.outboxsagalab.transfer.domain.TransferState;
import com.outboxsagalab.transfer.saga.CommandEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.UUID;

/**
 * Public REST surface for transfer-service.
 *
 *   POST /transfers       -- create a transfer, kick off the saga (returns 201)
 *   GET  /transfers/{id}  -- current state for inspection
 *
 * The POST is the one domain write that doesn't go through the saga itself --
 * it persists the transfer + writes the first DebitAccount outbox row in a
 * single transaction, mirroring the same outbox-first bootstrap used by every service in this lab.
 */
@RestController
@RequestMapping("/transfers")
public class TransferController {

    private static final Logger log = LoggerFactory.getLogger(TransferController.class);

    private final TransferRepository transfers;
    private final CommandEmitter commands;

    public TransferController(TransferRepository transfers, CommandEmitter commands) {
        this.transfers = transfers;
        this.commands = commands;
    }

    @PostMapping
    @Transactional
    public ResponseEntity<TransferResponse> create(@RequestBody CreateTransferRequest req) {
        UUID id = UUID.randomUUID();
        Transfer t = new Transfer(
                id,
                req.senderUser(), req.senderCurrency(),
                req.recipientUser(), req.recipientCurrency(),
                req.sourceAmount());
        transfers.save(t);
        log.info("Transfer created saga_id={} {}->{} {}->{} amount={}",
                id, req.senderUser(), req.recipientUser(),
                req.senderCurrency(), req.recipientCurrency(), req.sourceAmount());

        commands.debitAccount(t);
        t.transitionTo(TransferState.DEBIT_REQUESTED);
        log.info("Saga state {} -> {} saga_id={}",
                TransferState.PENDING, TransferState.DEBIT_REQUESTED, id);

        return ResponseEntity
                .created(URI.create("/transfers/" + id))
                .body(TransferResponse.from(t));
    }

    @GetMapping("/{id}")
    public ResponseEntity<TransferResponse> get(@PathVariable("id") UUID id) {
        return transfers.findById(id)
                .map(TransferResponse::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
