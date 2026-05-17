import 'package:flutter/material.dart';
import '../providers/dashboard_provider.dart';

class SymbolSelector extends StatelessWidget {
  final DashboardProvider provider;

  const SymbolSelector({super.key, required this.provider});

  @override
  Widget build(BuildContext context) {
    return SizedBox(
      height: 40,
      child: ListView.separated(
        scrollDirection: Axis.horizontal,
        itemCount: DashboardProvider.kSymbols.length,
        separatorBuilder: (_, __) => const SizedBox(width: 8),
        itemBuilder: (context, i) {
          final symbol = DashboardProvider.kSymbols[i];
          final isSelected = symbol == provider.selectedSymbol;
          final base = symbol.replaceAll('USDT', '');

          return GestureDetector(
            onTap: () => provider.selectSymbol(symbol),
            child: AnimatedContainer(
              duration: const Duration(milliseconds: 200),
              padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
              decoration: BoxDecoration(
                color: isSelected
                    ? const Color(0xFF00D4AA)
                    : const Color(0xFF21262D),
                borderRadius: BorderRadius.circular(20),
                border: Border.all(
                  color: isSelected
                      ? const Color(0xFF00D4AA)
                      : const Color(0xFF30363D),
                  width: 1,
                ),
              ),
              child: Text(
                base,
                style: TextStyle(
                  color: isSelected
                      ? const Color(0xFF0D1117)
                      : const Color(0xFF8B949E),
                  fontWeight:
                      isSelected ? FontWeight.w700 : FontWeight.w500,
                  fontSize: 13,
                  letterSpacing: 0.5,
                ),
              ),
            ),
          );
        },
      ),
    );
  }
}
