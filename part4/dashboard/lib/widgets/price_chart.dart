import 'package:flutter/material.dart';
import 'package:fl_chart/fl_chart.dart';
import 'package:intl/intl.dart';
import '../models/vwap_bar.dart';

class PriceChart extends StatelessWidget {
  final List<VwapBar> bars;

  const PriceChart({super.key, required this.bars});

  static const _kGreen = Color(0xFF00D4AA);
  static const _kBg    = Color(0xFF0D1117);

  @override
  Widget build(BuildContext context) {
    if (bars.length < 2) {
      return _placeholder(bars.isEmpty
          ? 'No VWAP data yet — HBase sink must be running'
          : 'Waiting for more data points…');
    }

    final spots = bars
        .asMap()
        .entries
        .map((e) => FlSpot(e.key.toDouble(), e.value.vwapUsd))
        .toList();

    final prices  = bars.map((b) => b.vwapUsd).toList();
    final minY    = prices.reduce((a, b) => a < b ? a : b);
    final maxY    = prices.reduce((a, b) => a > b ? a : b);
    final padding = (maxY - minY) == 0 ? 1.0 : (maxY - minY) * 0.05;

    final priceFmt = NumberFormat('#,##0.##');
    final interval = _labelInterval(bars.length);

    return Container(
      height: 240,
      decoration: BoxDecoration(
        color: const Color(0xFF161B22),
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: const Color(0xFF30363D)),
      ),
      padding: const EdgeInsets.fromLTRB(8, 16, 16, 8),
      child: LineChart(
        LineChartData(
          backgroundColor: _kBg,
          minY: minY - padding,
          maxY: maxY + padding,
          lineBarsData: [
            LineChartBarData(
              spots: spots,
              isCurved: true,
              curveSmoothness: 0.25,
              color: _kGreen,
              barWidth: 2,
              isStrokeCapRound: true,
              dotData: const FlDotData(show: false),
              belowBarData: BarAreaData(
                show: true,
                gradient: LinearGradient(
                  colors: [
                    _kGreen.withOpacity(0.18),
                    _kGreen.withOpacity(0.0),
                  ],
                  begin: Alignment.topCenter,
                  end: Alignment.bottomCenter,
                ),
              ),
            ),
          ],
          titlesData: FlTitlesData(
            leftTitles: AxisTitles(
              sideTitles: SideTitles(
                showTitles: true,
                reservedSize: 72,
                getTitlesWidget: (value, meta) => Padding(
                  padding: const EdgeInsets.only(right: 6),
                  child: Text(
                    priceFmt.format(value),
                    style: const TextStyle(
                      color: Color(0xFF8B949E),
                      fontSize: 10,
                    ),
                  ),
                ),
              ),
            ),
            bottomTitles: AxisTitles(
              sideTitles: SideTitles(
                showTitles: true,
                reservedSize: 28,
                interval: interval,
                getTitlesWidget: (value, meta) {
                  final idx = value.toInt();
                  if (idx < 0 || idx >= bars.length) {
                    return const SizedBox.shrink();
                  }
                  return Padding(
                    padding: const EdgeInsets.only(top: 6),
                    child: Text(
                      bars[idx].timeLabel,
                      style: const TextStyle(
                        color: Color(0xFF8B949E),
                        fontSize: 10,
                      ),
                    ),
                  );
                },
              ),
            ),
            topTitles:   const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            rightTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
          ),
          gridData: FlGridData(
            show: true,
            getDrawingHorizontalLine: (_) => const FlLine(
              color: Color(0xFF21262D),
              strokeWidth: 1,
            ),
            getDrawingVerticalLine: (_) => const FlLine(
              color: Color(0xFF21262D),
              strokeWidth: 1,
            ),
          ),
          borderData: FlBorderData(show: false),
          lineTouchData: LineTouchData(
            touchTooltipData: LineTouchTooltipData(
              getTooltipColor: (_) => const Color(0xFF21262D),
              tooltipRoundedRadius: 8,
              getTooltipItems: (touchedSpots) => touchedSpots.map((spot) {
                final idx = spot.x.toInt();
                final bar = (idx >= 0 && idx < bars.length) ? bars[idx] : null;
                return LineTooltipItem(
                  bar != null
                      ? '\$${priceFmt.format(spot.y)}\n${bar.timeLabel}'
                      : '\$${priceFmt.format(spot.y)}',
                  const TextStyle(
                    color: _kGreen,
                    fontWeight: FontWeight.w600,
                    fontSize: 12,
                  ),
                );
              }).toList(),
            ),
          ),
        ),
      ),
    );
  }

  double _labelInterval(int count) {
    if (count <= 10) return 1;
    if (count <= 20) return 2;
    if (count <= 40) return 5;
    return 10;
  }

  Widget _placeholder(String msg) {
    return Container(
      height: 240,
      decoration: BoxDecoration(
        color: const Color(0xFF161B22),
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: const Color(0xFF30363D)),
      ),
      child: Center(
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            const Icon(Icons.show_chart, color: Color(0xFF30363D), size: 36),
            const SizedBox(height: 8),
            Text(
              msg,
              style: const TextStyle(color: Color(0xFF8B949E), fontSize: 13),
              textAlign: TextAlign.center,
            ),
          ],
        ),
      ),
    );
  }
}
