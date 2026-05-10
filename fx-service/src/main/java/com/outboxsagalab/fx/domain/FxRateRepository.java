package com.outboxsagalab.fx.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface FxRateRepository extends JpaRepository<FxRate, FxRate.PK> {
    Optional<FxRate> findByBaseCurrencyAndQuoteCurrency(String base, String quote);
}
