import 'package:intl/intl.dart';

class VwapBar {
  final String symbol;
  final DateTime windowStart;
  final DateTime windowEnd;
  final double vwapUsd;
  final double totalVolume;
  final double totalNotional;
  final int tradeCount;
  final double lowUsd;
  final double highUsd;

  VwapBar({
    required this.symbol,
    required this.windowStart,
    required this.windowEnd,
    required this.vwapUsd,
    required this.totalVolume,
    required this.totalNotional,
    required this.tradeCount,
    required this.lowUsd,
    required this.highUsd,
  });

  factory VwapBar.fromJson(Map<String, dynamic> json) {
    return VwapBar(
      symbol:        json['symbol']        as String,
      windowStart:   _parseTs(json['window_start'] as String? ?? ''),
      windowEnd:     _parseTs(json['window_end']   as String? ?? ''),
      vwapUsd:       (json['vwap_usd']       as num).toDouble(),
      totalVolume:   (json['total_volume']   as num).toDouble(),
      totalNotional: (json['total_notional'] as num).toDouble(),
      tradeCount:    (json['trade_count']    as num).toInt(),
      lowUsd:        (json['low_usd']        as num).toDouble(),
      highUsd:       (json['high_usd']       as num).toDouble(),
    );
  }

  static DateTime _parseTs(String s) {
    if (s.isEmpty) return DateTime.now();
    try {
      return DateTime.parse(s).toLocal();
    } catch (_) {
      return DateTime.now();
    }
  }

  String get timeLabel => DateFormat('HH:mm').format(windowStart);
}
