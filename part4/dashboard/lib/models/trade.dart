class Trade {
  final String symbol;
  final String tradeId;
  final double price;
  final double qty;
  final double notional;
  final String side; // "BUY" or "SELL"
  final DateTime tradeTs;

  Trade({
    required this.symbol,
    required this.tradeId,
    required this.price,
    required this.qty,
    required this.notional,
    required this.side,
    required this.tradeTs,
  });

  factory Trade.fromJson(Map<String, dynamic> json) {
    return Trade(
      symbol:   json['symbol']   as String,
      tradeId:  json['trade_id'] as String,
      price:    (json['price']    as num).toDouble(),
      qty:      (json['qty']      as num).toDouble(),
      notional: (json['notional'] as num).toDouble(),
      side:     json['side']     as String,
      tradeTs:  _parseTs(json['trade_ts'] as String? ?? ''),
    );
  }

  static DateTime _parseTs(String s) {
    if (s.isEmpty) return DateTime.now();
    // trade_ts is epoch ms from Binance
    final ms = int.tryParse(s);
    if (ms != null) return DateTime.fromMillisecondsSinceEpoch(ms).toLocal();
    try {
      return DateTime.parse(s).toLocal();
    } catch (_) {
      return DateTime.now();
    }
  }

  bool get isBuy => side == 'BUY';
}
