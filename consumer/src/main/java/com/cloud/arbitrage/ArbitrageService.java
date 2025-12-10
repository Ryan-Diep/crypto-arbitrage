package com.cloud.arbitrage;

import crypto.TickerOuterClass.Ticker;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
public class ArbitrageService {
    
    private static final Logger log = LoggerFactory.getLogger(ArbitrageService.class);

    private static final double MIN_SPREAD_THRESHOLD = 0.5; 
    private static final int STALE_DATA_MS = 5000; 
    private static final int MAX_PRICE_UPDATES = 100;
    
    private final Map<String, Map<String, ExchangePrice>> prices = new ConcurrentHashMap<>();
    private final Map<String, ArbitrageOpportunity> opportunities = new ConcurrentHashMap<>();
    private final Deque<PriceUpdate> recentUpdates = new ConcurrentLinkedDeque<>();
    private final AtomicLong messageCount = new AtomicLong(0);
    private final AtomicLong arbitrageChecks = new AtomicLong(0);

    @KafkaListener(topics = "crypto-prices", groupId = "arbitrage-group")
    public void consume(List<byte[]> messages) {
        long startTime = System.currentTimeMillis();
        int processedCount = 0;
        
        try {
            for (byte[] message : messages) {
                try {
                    Ticker ticker = Ticker.parseFrom(message);
                    long currentTime = System.currentTimeMillis();
                    
                    ExchangePrice exchangePrice = new ExchangePrice(
                        ticker.getPrice(), 
                        ticker.getTimestamp(), 
                        currentTime
                    );
                    
                    prices.computeIfAbsent(ticker.getSymbol(), k -> new ConcurrentHashMap<>())
                          .put(ticker.getSource(), exchangePrice);
                    
                    if (processedCount % 10 == 0) {
                        PriceUpdate update = new PriceUpdate(
                            ticker.getSymbol(),
                            ticker.getSource(),
                            ticker.getPrice(),
                            ticker.getTimestamp()
                        );
                        recentUpdates.addFirst(update);
                        while (recentUpdates.size() > MAX_PRICE_UPDATES) {
                            recentUpdates.removeLast();
                        }
                    }
                    
                    if (processedCount % 10 == 0) {
                        detectArbitrage(ticker.getSymbol(), currentTime);
                    }
                    
                    processedCount++;
                } catch (Exception e) {
                    log.error("Error processing individual message: {}", e.getMessage());
                }
            }
            
            long totalCount = messageCount.addAndGet(processedCount);
            long duration = System.currentTimeMillis() - startTime;
            
            if (totalCount % 5000 < processedCount) {
                log.info("Batch: {} msgs in {}ms | Total: {} | Symbols Tracked: {} | Active Opps: {}",
                    processedCount, duration, totalCount, prices.size(), opportunities.size());
            }
            
        } catch (Exception e) {
            log.error("Error processing batch: {}", e.getMessage(), e);
        }
    }
    
    private void detectArbitrage(String symbol, long currentTime) {
        arbitrageChecks.incrementAndGet();
        
        Map<String, ExchangePrice> exchangePrices = prices.get(symbol);
        if (exchangePrices == null || exchangePrices.size() < 2) {
            opportunities.remove(symbol);
            return;
        }

        String bestBuyExchange = null;
        double bestBuyPrice = Double.MAX_VALUE;
        
        String bestSellExchange = null;
        double bestSellPrice = -1.0;

        for (Map.Entry<String, ExchangePrice> entry : exchangePrices.entrySet()) {
            String exchange = entry.getKey();
            ExchangePrice data = entry.getValue();

            if (currentTime - data.receivedAt > STALE_DATA_MS) continue;
            if (data.price <= 0) continue;

            if (data.price < bestBuyPrice) {
                bestBuyPrice = data.price;
                bestBuyExchange = exchange;
            }
            if (data.price > bestSellPrice) {
                bestSellPrice = data.price;
                bestSellExchange = exchange;
            }
        }

        if (bestBuyExchange == null || bestSellExchange == null || bestBuyExchange.equals(bestSellExchange)) {
            opportunities.remove(symbol);
            return;
        }

        double priceDiff = bestSellPrice - bestBuyPrice;
        if (priceDiff <= 0) {
             opportunities.remove(symbol);
             return;
        }

        double spreadPercent = (priceDiff / bestBuyPrice) * 100;

        if (spreadPercent >= MIN_SPREAD_THRESHOLD) {
            ArbitrageOpportunity opp = new ArbitrageOpportunity(
                symbol,
                bestBuyExchange,
                bestSellExchange,
                bestBuyPrice,
                bestSellPrice,
                spreadPercent,
                priceDiff,
                currentTime
            );
            opportunities.put(symbol, opp);
            
            log.info("OPPORTUNITY: {} | {}% Spread | Buy {} ${} -> Sell {} ${}", 
                symbol, String.format("%.2f", spreadPercent), 
                bestBuyExchange, bestBuyPrice, bestSellExchange, bestSellPrice);
        } else {
            opportunities.remove(symbol);
        }
    }
    
    public Map<String, Object> getOpportunitiesData() {
        long currentTime = System.currentTimeMillis();
        
        opportunities.entrySet().removeIf(entry -> 
            currentTime - entry.getValue().timestamp > STALE_DATA_MS);
        
        List<Map<String, Object>> oppList = opportunities.values().stream()
            .sorted(Comparator.comparingDouble(ArbitrageOpportunity::getSpreadPercent).reversed())
            .limit(20)
            .map(opp -> {
                Map<String, Object> map = new HashMap<>();
                map.put("symbol", opp.symbol);
                map.put("buyExchange", opp.buyExchange);
                map.put("sellExchange", opp.sellExchange);
                map.put("buyPrice", opp.buyPrice);
                map.put("sellPrice", opp.sellPrice);
                map.put("spread", opp.spreadPercent);
                map.put("priceDiff", opp.priceDiff);
                map.put("timestamp", opp.timestamp);
                return map;
            })
            .collect(Collectors.toList());
        
        double avgSpread = opportunities.values().stream()
            .mapToDouble(ArbitrageOpportunity::getSpreadPercent)
            .average().orElse(0.0);
        
        double maxSpread = opportunities.values().stream()
            .mapToDouble(ArbitrageOpportunity::getSpreadPercent)
            .max().orElse(0.0);
        
        Map<String, Object> stats = new HashMap<>();
        stats.put("avgSpread", avgSpread);
        stats.put("maxSpread", maxSpread);
        stats.put("trackedSymbols", prices.size());
        stats.put("activeOpportunities", opportunities.size());
        
        Map<String, Object> result = new HashMap<>();
        result.put("opportunities", oppList);
        result.put("stats", stats);
        result.put("lastUpdate", currentTime);
        
        return result;
    }
    
    public Map<String, Object> getAllPricesData() {
        long currentTime = System.currentTimeMillis();
        
        List<Map<String, Object>> pricesList = new ArrayList<>();

        for (Map.Entry<String, Map<String, ExchangePrice>> entry : prices.entrySet()) {
            String symbol = entry.getKey();
            Map<String, ExchangePrice> exchangeMap = entry.getValue();
            
            Map<String, Object> row = new HashMap<>();
            row.put("symbol", symbol);
            
            double minP = Double.MAX_VALUE;
            double maxP = -1.0;
            int validCount = 0;
            
            for (Map.Entry<String, ExchangePrice> exEntry : exchangeMap.entrySet()) {
                String exName = exEntry.getKey();
                ExchangePrice pData = exEntry.getValue();
                
                row.put(exName, pData.price);
                row.put(exName + "_age", currentTime - pData.receivedAt);
                
                if (exName.equals("COINBASE")) {
                    row.put("coinbasePrice", pData.price);
                    row.put("coinbaseAge", currentTime - pData.receivedAt);
                } else if (exName.equals("BINANCE")) {
                    row.put("binancePrice", pData.price);
                    row.put("binanceAge", currentTime - pData.receivedAt);
                }

                if (currentTime - pData.receivedAt <= STALE_DATA_MS) {
                   validCount++;
                   if (pData.price < minP) minP = pData.price;
                   if (pData.price > maxP) maxP = pData.price;
                }
            }
            
            if (validCount >= 2 && maxP > minP) {
                double spread = ((maxP - minP) / minP) * 100;
                row.put("spreadPercent", spread);
                row.put("bothFresh", true);
            } else {
                row.put("spreadPercent", 0.0);
                row.put("bothFresh", false);
            }
            
            pricesList.add(row);
        }
        
        pricesList.sort(Comparator.comparing(m -> (String) m.get("symbol")));
        
        List<Map<String, Object>> updates = recentUpdates.stream()
            .limit(100)
            .map(update -> {
                Map<String, Object> map = new HashMap<>();
                map.put("symbol", update.symbol);
                map.put("exchange", update.exchange);
                map.put("price", update.price);
                map.put("timestamp", update.timestamp);
                return map;
            })
            .collect(Collectors.toList());
        
        Map<String, Object> result = new HashMap<>();
        result.put("prices", pricesList);
        result.put("updates", updates);
        result.put("totalSymbols", prices.size());
        result.put("lastUpdate", currentTime);
        
        return result;
    }
    
    private static class ExchangePrice {
        final double price;
        final long timestamp;
        final long receivedAt;
        
        ExchangePrice(double price, long timestamp, long receivedAt) {
            this.price = price;
            this.timestamp = timestamp;
            this.receivedAt = receivedAt;
        }
    }
    
    private static class PriceUpdate {
        final String symbol;
        final String exchange;
        final double price;
        final long timestamp;
        
        PriceUpdate(String symbol, String exchange, double price, long timestamp) {
            this.symbol = symbol;
            this.exchange = exchange;
            this.price = price;
            this.timestamp = timestamp;
        }
    }
    
    private static class ArbitrageOpportunity {
        final String symbol;
        final String buyExchange;
        final String sellExchange;
        final double buyPrice;
        final double sellPrice;
        final double spreadPercent;
        final double priceDiff;
        final long timestamp;
        
        ArbitrageOpportunity(String symbol, String buyExchange, String sellExchange,
                             double buyPrice, double sellPrice, double spreadPercent, 
                             double priceDiff, long timestamp) {
            this.symbol = symbol;
            this.buyExchange = buyExchange;
            this.sellExchange = sellExchange;
            this.buyPrice = buyPrice;
            this.sellPrice = sellPrice;
            this.spreadPercent = spreadPercent;
            this.priceDiff = priceDiff;
            this.timestamp = timestamp;
        }
        
        double getSpreadPercent() { return spreadPercent; }
    }
}