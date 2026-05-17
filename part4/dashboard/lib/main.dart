import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'providers/dashboard_provider.dart';
import 'screens/dashboard_screen.dart';

void main() {
  runApp(const BinanceDashboardApp());
}

class BinanceDashboardApp extends StatelessWidget {
  const BinanceDashboardApp({super.key});

  @override
  Widget build(BuildContext context) {
    return ChangeNotifierProvider(
      create: (_) => DashboardProvider()..init(),
      child: MaterialApp(
        title: 'Binance Live Dashboard',
        debugShowCheckedModeBanner: false,
        theme: ThemeData(
          brightness: Brightness.dark,
          colorScheme: ColorScheme.dark(
            primary: const Color(0xFF00D4AA),
            secondary: const Color(0xFF00B894),
            surface: const Color(0xFF161B22),
            onSurface: const Color(0xFFE6EDF3),
          ),
          scaffoldBackgroundColor: const Color(0xFF0D1117),
          cardTheme: CardThemeData(
            color: const Color(0xFF161B22),
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(12),
            ),
          ),
          textTheme: const TextTheme(
            bodyMedium: TextStyle(color: Color(0xFFE6EDF3)),
            bodySmall:  TextStyle(color: Color(0xFF8B949E)),
          ),
          useMaterial3: true,
        ),
        home: const DashboardScreen(),
      ),
    );
  }
}
