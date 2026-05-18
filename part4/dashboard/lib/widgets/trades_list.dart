import 'package:flutter/material.dart';
import 'package:intl/intl.dart';
import '../models/trade.dart';

class TradesList extends StatelessWidget {
  final List<Trade> trades;

  const TradesList({super.key, required this.trades});

  static final _priceFmt  = NumberFormat('#,##0.####');
  static final _notFmt    = NumberFormat('#,##0.00');
  static final _timeFmt   = DateFormat('HH:mm:ss');

  static const _kBuy  = Color(0xFF26D65B);
  static const _kSell = Color(0xFFFF4E4E);

  @override
  Widget build(BuildContext context) {
    if (trades.isEmpty) {
      return Container(
        height: 120,
        decoration: BoxDecoration(
          color: const Color(0xFF161B22),
          borderRadius: BorderRadius.circular(12),
          border: Border.all(color: const Color(0xFF30363D)),
        ),
        child: const Center(
          child: Text(
            'No trades yet',
            style: TextStyle(color: Color(0xFF8B949E)),
          ),
        ),
      );
    }

    return Container(
      decoration: BoxDecoration(
        color: const Color(0xFF161B22),
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: const Color(0xFF30363D)),
      ),
      child: Column(
        children: [
          _header(),
          const Divider(height: 1, color: Color(0xFF30363D)),
          ...trades.take(20).map(_row),
        ],
      ),
    );
  }

  Widget _header() {
    const style = TextStyle(
      color: Color(0xFF8B949E),
      fontSize: 11,
      letterSpacing: 0.5,
      fontWeight: FontWeight.w600,
    );
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      child: Row(
        children: const [
          SizedBox(width: 60, child: Text('SIDE',   style: style)),
          Expanded(child: Text('PRICE',              style: style, textAlign: TextAlign.right)),
          Expanded(child: Text('QTY',                style: style, textAlign: TextAlign.right)),
          Expanded(child: Text('NOTIONAL',           style: style, textAlign: TextAlign.right)),
          Expanded(child: Text('TIME',               style: style, textAlign: TextAlign.right)),
        ],
      ),
    );
  }

  Widget _row(Trade t) {
    final sideColor = t.isBuy ? _kBuy : _kSell;
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 6),
      decoration: BoxDecoration(
        border: Border(
          bottom: BorderSide(color: const Color(0xFF30363D), width: 0.5),
        ),
      ),
      child: Row(
        children: [
          SizedBox(
            width: 60,
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              mainAxisSize: MainAxisSize.min,
              children: [
                Container(
                  padding:
                      const EdgeInsets.symmetric(horizontal: 6, vertical: 2),
                  decoration: BoxDecoration(
                    color: sideColor.withOpacity(0.15),
                    borderRadius: BorderRadius.circular(4),
                  ),
                  child: Text(
                    t.side,
                    style: TextStyle(
                      color: sideColor,
                      fontSize: 10,
                      fontWeight: FontWeight.w700,
                      letterSpacing: 0.3,
                    ),
                    textAlign: TextAlign.center,
                  ),
                ),
                if (t.assetCategory.isNotEmpty &&
                    t.assetCategory != 'Unknown') ...[
                  const SizedBox(height: 2),
                  Text(
                    t.assetCategory,
                    style: const TextStyle(
                      color: Color(0xFF7C6AFA),
                      fontSize: 8,
                      letterSpacing: 0.2,
                    ),
                    overflow: TextOverflow.ellipsis,
                  ),
                ],
              ],
            ),
          ),
          Expanded(
            child: Text(
              '\$${_priceFmt.format(t.price)}',
              style: TextStyle(
                color: sideColor,
                fontSize: 12,
                fontFeatures: const [FontFeature.tabularFigures()],
              ),
              textAlign: TextAlign.right,
            ),
          ),
          Expanded(
            child: Text(
              t.qty.toStringAsFixed(6),
              style: const TextStyle(
                color: Color(0xFFE6EDF3),
                fontSize: 12,
                fontFeatures: [FontFeature.tabularFigures()],
              ),
              textAlign: TextAlign.right,
            ),
          ),
          Expanded(
            child: Text(
              '\$${_notFmt.format(t.notional)}',
              style: const TextStyle(
                color: Color(0xFFE6EDF3),
                fontSize: 12,
                fontFeatures: [FontFeature.tabularFigures()],
              ),
              textAlign: TextAlign.right,
            ),
          ),
          Expanded(
            child: Text(
              _timeFmt.format(t.tradeTs),
              style: const TextStyle(
                color: Color(0xFF8B949E),
                fontSize: 11,
                fontFeatures: [FontFeature.tabularFigures()],
              ),
              textAlign: TextAlign.right,
            ),
          ),
        ],
      ),
    );
  }
}
