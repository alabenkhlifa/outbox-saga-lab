package com.outboxsagalab.inventory.api;

import com.outboxsagalab.inventory.domain.Stock;
import com.outboxsagalab.inventory.domain.StockRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Debug-only read endpoint. Lets you eyeball current stock levels without
 * having to shell into the database. Not part of the saga contract.
 */
@RestController
@RequestMapping("/stock")
public class StockController {

    private final StockRepository stockRepository;

    public StockController(StockRepository stockRepository) {
        this.stockRepository = stockRepository;
    }

    @GetMapping
    public List<StockView> list() {
        return stockRepository.findAll().stream()
                .map(s -> new StockView(s.getSku(), s.getAvailableQty(), s.getUpdatedAt().toString()))
                .toList();
    }

    public record StockView(String sku, int availableQty, String updatedAt) {
        public static StockView of(Stock s) {
            return new StockView(s.getSku(), s.getAvailableQty(), s.getUpdatedAt().toString());
        }
    }
}
