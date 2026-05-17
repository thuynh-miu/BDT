class SymbolSummary {
  final String symbol;
  final double vwapUsd;
  final double highUsd;
  final double lowUsd;
  final int tradeCount;
  final double totalVolume;
  final String windowStart;

  SymbolSummary({
    required this.symbol,
    required this.vwapUsd,
    required this.highUsd,
    required this.lowUsd,
    required this.tradeCount,
    required this.totalVolume,
    required this.windowStart,
  });

  factory SymbolSummary.fromJson(Map<String, dynamic> json) {
    return SymbolSummary(
      symbol:       json['symbol']       as String,
      vwapUsd:      (json['vwap_usd']     as num).toDouble(),
      highUsd:      (json['high_usd']     as num).toDouble(),
      lowUsd:       (json['low_usd']      as num).toDouble(),
      tradeCount:   (json['trade_count']  as num).toInt(),
      totalVolume:  (json['total_volume'] as num).toDouble(),
      windowStart:  json['window_start']  as String? ?? '',
    );
  }

  String get baseCurrency => symbol.replaceAll('USDT', '');
}
