package com.outboxsagalab.account.api;

import com.outboxsagalab.account.domain.Wallet;
import com.outboxsagalab.account.domain.WalletRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Read-only debug surface for wallet balances. Mutations only happen via
 * Kafka commands.
 */
@RestController
@RequestMapping("/wallets")
public class WalletController {

    private final WalletRepository wallets;

    public WalletController(WalletRepository wallets) {
        this.wallets = wallets;
    }

    public record WalletView(UUID id, String userId, String currency, BigDecimal balance) {
        static WalletView from(Wallet w) {
            return new WalletView(w.getId(), w.getUserId(), w.getCurrency(), w.getBalance());
        }
    }

    @GetMapping("/{user}/{currency}")
    public ResponseEntity<WalletView> get(@PathVariable("user") String user,
                                          @PathVariable("currency") String currency) {
        return wallets.findByUserIdAndCurrency(user, currency)
                .map(WalletView::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
