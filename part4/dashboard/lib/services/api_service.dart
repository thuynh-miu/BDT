import 'dart:convert';
import 'package:http/http.dart' as http;
import '../models/vwap_bar.dart';
import '../models/trade.dart';
import '../models/symbol_summary.dart';

class ApiService {
  final String baseUrl;

  ApiService({this.baseUrl = 'http://localhost:8000'});

  Future<List<VwapBar>> fetchVwap(String symbol, {int minutes = 60}) async {
    final uri = Uri.parse('$baseUrl/api/vwap/$symbol?minutes=$minutes');
    final response = await http.get(uri).timeout(const Duration(seconds: 15));
    if (response.statusCode == 200) {
      final List<dynamic> data = jsonDecode(response.body);
      return data.map((e) => VwapBar.fromJson(e as Map<String, dynamic>)).toList();
    }
    throw Exception('VWAP fetch failed: ${response.statusCode}');
  }

  Future<List<Trade>> fetchTrades(String symbol, {int limit = 30}) async {
    final uri = Uri.parse('$baseUrl/api/trades/$symbol?limit=$limit');
    final response = await http.get(uri).timeout(const Duration(seconds: 15));
    if (response.statusCode == 200) {
      final List<dynamic> data = jsonDecode(response.body);
      final trades = data
          .map((e) => Trade.fromJson(e as Map<String, dynamic>))
          .toList();
      return trades;
    }
    throw Exception('Trades fetch failed: ${response.statusCode}');
  }

  Future<List<SymbolSummary>> fetchSummary() async {
    final uri = Uri.parse('$baseUrl/api/summary');
    final response = await http.get(uri).timeout(const Duration(seconds: 15));
    if (response.statusCode == 200) {
      final List<dynamic> data = jsonDecode(response.body);
      return data
          .map((e) => SymbolSummary.fromJson(e as Map<String, dynamic>))
          .toList();
    }
    throw Exception('Summary fetch failed: ${response.statusCode}');
  }

  Future<bool> checkHealth() async {
    try {
      final response = await http
          .get(Uri.parse('$baseUrl/health'))
          .timeout(const Duration(seconds: 3));
      return response.statusCode == 200;
    } catch (_) {
      return false;
    }
  }
}
