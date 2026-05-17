import 'package:flutter/material.dart';
import 'package:fl_chart/fl_chart.dart';
import '../models/vwap_bar.dart';

class VolumeChart extends StatelessWidget {
  final List<VwapBar> bars;

  const VolumeChart({super.key, required this.bars});

  @override
  Widget build(BuildContext context) {
    if (bars.isEmpty) {
      return _placeholder('No volume data yet');
    }

    final maxVol = bars
        .map((b) => b.totalNotional)
        .reduce((a, b) => a > b ? a : b);

    if (maxVol == 0) return _placeholder('No volume data yet');

    final barGroups = bars.asMap().entries.map((e) {
      final pct = e.value.totalNotional / maxVol;
      return BarChartGroupData(
        x: e.key,
        barRods: [
          BarChartRodData(
            toY: e.value.totalNotional,
            color: Color.lerp(
              const Color(0xFF00D4AA).withOpacity(0.4),
              const Color(0xFF00D4AA),
              pct,
            ),
            width: _barWidth(bars.length),
            borderRadius: const BorderRadius.vertical(top: Radius.circular(2)),
          ),
        ],
      );
    }).toList();

    return Container(
      height: 120,
      decoration: BoxDecoration(
        color: const Color(0xFF161B22),
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: const Color(0xFF30363D)),
      ),
      padding: const EdgeInsets.fromLTRB(8, 12, 16, 8),
      child: BarChart(
        BarChartData(
          maxY: maxVol * 1.1,
          barGroups: barGroups,
          barTouchData: BarTouchData(
            touchTooltipData: BarTouchTooltipData(
              getTooltipColor: (_) => const Color(0xFF21262D),
              getTooltipItem: (group, groupIndex, rod, rodIndex) {
                return BarTooltipItem(
                  _fmt(rod.toY),
                  const TextStyle(
                    color: Color(0xFF00D4AA),
                    fontSize: 11,
                    fontWeight: FontWeight.w600,
                  ),
                );
              },
            ),
          ),
          titlesData: FlTitlesData(
            leftTitles: AxisTitles(
              sideTitles: SideTitles(
                showTitles: true,
                reservedSize: 72,
                getTitlesWidget: (value, meta) => Padding(
                  padding: const EdgeInsets.only(right: 6),
                  child: Text(
                    _fmt(value),
                    style: const TextStyle(
                      color: Color(0xFF8B949E),
                      fontSize: 10,
                    ),
                  ),
                ),
              ),
            ),
            bottomTitles: const AxisTitles(
              sideTitles: SideTitles(showTitles: false),
            ),
            topTitles:   const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            rightTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
          ),
          gridData: FlGridData(
            show: true,
            drawVerticalLine: false,
            getDrawingHorizontalLine: (_) => const FlLine(
              color: Color(0xFF21262D),
              strokeWidth: 1,
            ),
          ),
          borderData: FlBorderData(show: false),
        ),
      ),
    );
  }

  double _barWidth(int count) {
    if (count <= 10) return 16;
    if (count <= 30) return 8;
    if (count <= 60) return 5;
    return 3;
  }

  String _fmt(double v) {
    if (v >= 1e9) return '${(v / 1e9).toStringAsFixed(1)}B';
    if (v >= 1e6) return '${(v / 1e6).toStringAsFixed(1)}M';
    if (v >= 1e3) return '${(v / 1e3).toStringAsFixed(1)}K';
    return v.toStringAsFixed(0);
  }

  Widget _placeholder(String msg) {
    return Container(
      height: 120,
      decoration: BoxDecoration(
        color: const Color(0xFF161B22),
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: const Color(0xFF30363D)),
      ),
      child: Center(
        child: Text(
          msg,
          style: const TextStyle(color: Color(0xFF8B949E), fontSize: 13),
        ),
      ),
    );
  }
}
