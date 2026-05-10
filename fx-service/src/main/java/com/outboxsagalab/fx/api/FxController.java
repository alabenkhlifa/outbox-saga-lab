package com.outboxsagalab.fx.api;

import com.outboxsagalab.fx.domain.FxRate;
import com.outboxsagalab.fx.domain.FxRateRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;

@RestController
@RequestMapping("/fx")
public class FxController {

    private final FxRateRepository rates;

    public FxController(FxRateRepository rates) {
        this.rates = rates;
    }

    public record RateView(String base, String quote, BigDecimal rate) { }

    @GetMapping("/rates")
    public List<RateView> rates() {
        return rates.findAll().stream()
                .map(r -> new RateView(r.getBaseCurrency(), r.getQuoteCurrency(), r.getRate()))
                .toList();
    }
}
