import 'dart:async';
import 'package:flutter/foundation.dart';
import '../models/vwap_bar.dart';
import '../models/trade.dart';
import '../models/symbol_summary.dart';
import '../services/api_service.dart';

class DashboardProvider extends ChangeNotifier {
  final ApiService _api = ApiService();
  Timer? _timer;

  static const List<String> kSymbols = [
    'BTCUSDT',
    'ETHUSDT',
    'BNBUSDT',
    'SOLUSDT',
    'XRPUSDT',
  ];

  String _selectedSymbol = 'BTCUSDT';
  List<VwapBar> _vwapBars = [];
  List<Trade> _trades = [];
  List<SymbolSummary> _summary = [];
  bool _isLoading = false;
  bool _isConnected = false;
  String? _error;
  DateTime? _lastUpdate;

  // ── Getters ──────────────────────────────────────────────────────────────

  String get selectedSymbol => _selectedSymbol;
  List<VwapBar> get vwapBars => _vwapBars;
  List<Trade> get trades => _trades;
  List<SymbolSummary> get summary => _summary;
  bool get isLoading => _isLoading;
  bool get isConnected => _isConnected;
  String? get error => _error;
  DateTime? get lastUpdate => _lastUpdate;

  SymbolSummary? get currentSummary {
    try {
      return _summary.firstWhere((s) => s.symbol == _selectedSymbol);
    } catch (_) {
      return null;
    }
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  void init() {
    _refresh();
    _timer = Timer.periodic(const Duration(seconds: 5), (_) => _refresh());
  }

  void selectSymbol(String symbol) {
    if (_selectedSymbol == symbol) return;
    _selectedSymbol = symbol;
    _vwapBars = [];
    _trades = [];
    notifyListeners();
    _refresh();
  }

  Future<void> manualRefresh() => _refresh();

  // ── Data fetch ────────────────────────────────────────────────────────────

  Future<void> _refresh() async {
    if (_isLoading) return;
    _isLoading = true;
    _error = null;
    notifyListeners();

    try {
      final results = await Future.wait([
        _api.fetchVwap(_selectedSymbol, minutes: 60),
        _api.fetchTrades(_selectedSymbol, limit: 30),
        _api.fetchSummary(),
      ]);
      _vwapBars  = results[0] as List<VwapBar>;
      _trades    = results[1] as List<Trade>;
      _summary   = results[2] as List<SymbolSummary>;
      _isConnected = true;
      _lastUpdate  = DateTime.now();
    } catch (e) {
      _error = e.toString();
      _isConnected = false;
    }

    _isLoading = false;
    notifyListeners();
  }

  @override
  void dispose() {
    _timer?.cancel();
    super.dispose();
  }
}
