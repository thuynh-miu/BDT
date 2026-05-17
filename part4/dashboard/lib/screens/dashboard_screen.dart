import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:intl/intl.dart';
import '../providers/dashboard_provider.dart';
import '../widgets/symbol_selector.dart';
import '../widgets/price_chart.dart';
import '../widgets/volume_chart.dart';
import '../widgets/trades_list.dart';
import '../widgets/stat_card.dart';

class DashboardScreen extends StatelessWidget {
  const DashboardScreen({super.key});

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xFF0D1117),
      body: SafeArea(
        child: Consumer<DashboardProvider>(
          builder: (context, provider, _) {
            return Column(
              crossAxisAlignment: CrossAxisAlignment.stretch,
              children: [
                _AppHeader(provider: provider),
                if (provider.error != null) _ErrorBanner(message: provider.error!),
                Expanded(
                  child: SingleChildScrollView(
                    padding: const EdgeInsets.all(16),
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        SymbolSelector(provider: provider),
                        const SizedBox(height: 16),
                        _StatsRow(provider: provider),
                        const SizedBox(height: 20),
                        _SectionLabel('VWAP Price  (1-min bars, last 60 min)'),
                        const SizedBox(height: 8),
                        PriceChart(bars: provider.vwapBars),
                        const SizedBox(height: 16),
                        _SectionLabel('Notional Volume  (USD per bar)'),
                        const SizedBox(height: 8),
                        VolumeChart(bars: provider.vwapBars),
                        const SizedBox(height: 20),
                        _SectionLabel('Recent Trades  (newest first)'),
                        const SizedBox(height: 8),
                        TradesList(trades: provider.trades),
                        const SizedBox(height: 24),
                      ],
                    ),
                  ),
                ),
              ],
            );
          },
        ),
      ),
    );
  }
}

// ── AppHeader ────────────────────────────────────────────────────────────────

class _AppHeader extends StatelessWidget {
  final DashboardProvider provider;
  const _AppHeader({required this.provider});

  @override
  Widget build(BuildContext context) {
    final timeFmt = DateFormat('HH:mm:ss');
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
      decoration: const BoxDecoration(
        color: Color(0xFF161B22),
        border: Border(bottom: BorderSide(color: Color(0xFF30363D))),
      ),
      child: Row(
        children: [
          const Text(
            'Binance',
            style: TextStyle(
              color: Color(0xFFE6EDF3),
              fontSize: 18,
              fontWeight: FontWeight.w700,
              letterSpacing: 0.3,
            ),
          ),
          const SizedBox(width: 4),
          const Text(
            'Live Dashboard',
            style: TextStyle(
              color: Color(0xFF8B949E),
              fontSize: 18,
              fontWeight: FontWeight.w400,
            ),
          ),
          const Spacer(),
          if (provider.lastUpdate != null)
            Text(
              timeFmt.format(provider.lastUpdate!),
              style: const TextStyle(
                color: Color(0xFF8B949E),
                fontSize: 12,
                fontFeatures: [FontFeature.tabularFigures()],
              ),
            ),
          const SizedBox(width: 12),
          _LiveDot(isConnected: provider.isConnected),
          const SizedBox(width: 12),
          _RefreshButton(
            isLoading: provider.isLoading,
            onPressed: provider.manualRefresh,
          ),
        ],
      ),
    );
  }
}

class _LiveDot extends StatelessWidget {
  final bool isConnected;
  const _LiveDot({required this.isConnected});

  @override
  Widget build(BuildContext context) {
    return Row(
      mainAxisSize: MainAxisSize.min,
      children: [
        Container(
          width: 8,
          height: 8,
          decoration: BoxDecoration(
            shape: BoxShape.circle,
            color: isConnected ? const Color(0xFF26D65B) : const Color(0xFFFF4E4E),
            boxShadow: isConnected
                ? [
                    BoxShadow(
                      color: const Color(0xFF26D65B).withOpacity(0.5),
                      blurRadius: 6,
                    ),
                  ]
                : null,
          ),
        ),
        const SizedBox(width: 5),
        Text(
          isConnected ? 'LIVE' : 'OFF',
          style: TextStyle(
            color: isConnected ? const Color(0xFF26D65B) : const Color(0xFFFF4E4E),
            fontSize: 11,
            fontWeight: FontWeight.w700,
            letterSpacing: 0.5,
          ),
        ),
      ],
    );
  }
}

class _RefreshButton extends StatelessWidget {
  final bool isLoading;
  final VoidCallback onPressed;
  const _RefreshButton({required this.isLoading, required this.onPressed});

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      onTap: isLoading ? null : onPressed,
      child: Container(
        width: 32,
        height: 32,
        decoration: BoxDecoration(
          color: const Color(0xFF21262D),
          borderRadius: BorderRadius.circular(8),
          border: Border.all(color: const Color(0xFF30363D)),
        ),
        child: isLoading
            ? const Padding(
                padding: EdgeInsets.all(8),
                child: CircularProgressIndicator(
                  strokeWidth: 1.5,
                  color: Color(0xFF00D4AA),
                ),
              )
            : const Icon(
                Icons.refresh,
                size: 16,
                color: Color(0xFF8B949E),
              ),
      ),
    );
  }
}

// ── Error banner ──────────────────────────────────────────────────────────────

class _ErrorBanner extends StatelessWidget {
  final String message;
  const _ErrorBanner({required this.message});

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 10),
      color: const Color(0xFF3D1C1C),
      child: Row(
        children: [
          const Icon(Icons.warning_amber, color: Color(0xFFFF6B6B), size: 16),
          const SizedBox(width: 8),
          Expanded(
            child: Text(
              'API error — check that the API server and HBase are running.\n$message',
              style: const TextStyle(color: Color(0xFFFF6B6B), fontSize: 12),
            ),
          ),
        ],
      ),
    );
  }
}

// ── Stats row ─────────────────────────────────────────────────────────────────

class _StatsRow extends StatelessWidget {
  final DashboardProvider provider;
  const _StatsRow({required this.provider});

  @override
  Widget build(BuildContext context) {
    final s = provider.currentSummary;
    final priceFmt = NumberFormat('#,##0.##');
    final volFmt   = _fmtVol;

    final vwap  = s != null ? '\$${priceFmt.format(s.vwapUsd)}' : '--';
    final high  = s != null ? '\$${priceFmt.format(s.highUsd)}' : '--';
    final low   = s != null ? '\$${priceFmt.format(s.lowUsd)}' : '--';
    final vol   = s != null ? volFmt(s.totalVolume)              : '--';
    final count = s != null ? s.tradeCount.toString()            : '--';

    return Wrap(
      spacing: 8,
      runSpacing: 8,
      children: [
        StatCard(
          label: 'VWAP (1-min)',
          value: vwap,
          valueColor: const Color(0xFF00D4AA),
          icon: Icons.price_change_outlined,
        ),
        StatCard(
          label: '24H HIGH',
          value: high,
          valueColor: const Color(0xFF26D65B),
          icon: Icons.arrow_upward,
        ),
        StatCard(
          label: '24H LOW',
          value: low,
          valueColor: const Color(0xFFFF4E4E),
          icon: Icons.arrow_downward,
        ),
        StatCard(
          label: 'VOLUME',
          value: vol,
          icon: Icons.bar_chart,
        ),
        StatCard(
          label: 'TRADES',
          value: count,
          icon: Icons.swap_horiz,
        ),
      ],
    );
  }

  String Function(double) get _fmtVol => (v) {
    if (v >= 1e6) return '${(v / 1e6).toStringAsFixed(2)}M';
    if (v >= 1e3) return '${(v / 1e3).toStringAsFixed(2)}K';
    return v.toStringAsFixed(4);
  };
}

// ── Section label ─────────────────────────────────────────────────────────────

class _SectionLabel extends StatelessWidget {
  final String text;
  const _SectionLabel(this.text);

  @override
  Widget build(BuildContext context) {
    return Text(
      text.toUpperCase(),
      style: const TextStyle(
        color: Color(0xFF8B949E),
        fontSize: 11,
        fontWeight: FontWeight.w600,
        letterSpacing: 0.8,
      ),
    );
  }
}
