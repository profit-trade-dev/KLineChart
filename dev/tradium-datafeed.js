/**
 * TradiumDatafeed — UDF REST + WebSocket adapter
 * Implements klinecharts-pro Datafeed interface:
 *   searchSymbols, getHistoryKLineData, subscribe, unsubscribe
 */

(function (global) {
  'use strict';

  var FRAME_TYPES = { ERROR: 1, CANDLES: 5 };

  // ==========================================
  // Shared Token Refresh State & Helpers
  // ==========================================

  var _isLocalhost = typeof window !== 'undefined' &&
    (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1');

  var DEFAULT_REFRESH_TOKEN_URL = 'https://api-user-management-cug.tradesea.ai/v1/login/refresh';

  var MIN_REFRESH_INTERVAL = 5000; // 5 seconds between refresh attempts

  // Shared across all TradiumDatafeed instances
  var _isRefreshing   = false;
  var _refreshPromise = null;
  var _lastRefreshTime = 0;

  /**
   * Get the refresh token from localStorage
   */
  function _getRefreshToken() {
    try { return localStorage.getItem('refresh_token'); } catch (e) { return null; }
  }

  /**
   * Store tokens in localStorage
   */
  function _storeTokens(refreshToken, accessToken) {
    try {
      if (refreshToken) localStorage.setItem('refresh_token', refreshToken);
      if (accessToken)  localStorage.setItem('access_token', accessToken);
    } catch (e) {
      console.error('[TokenRefresh] Failed to store tokens:', e);
    }
  }

  /**
   * Perform a token refresh request.
   * De-duplicates concurrent calls — all callers share one in-flight request.
   * Returns Promise<{ success: boolean, error?: Error, data?: object }>
   */
  function _doTokenRefresh(refreshUrl) {
    var now = Date.now();
    if (now - _lastRefreshTime < MIN_REFRESH_INTERVAL) {
      console.log('[TokenRefresh] Rate limited, skipping refresh');
      return Promise.resolve({ success: false, error: new Error('Rate limited') });
    }

    if (_isRefreshing && _refreshPromise) {
      console.log('[TokenRefresh] Already refreshing, waiting...');
      return _refreshPromise;
    }

    _isRefreshing = true;
    _lastRefreshTime = now;

    _refreshPromise = new Promise(function (resolve) {
      console.log('[TokenRefresh] Refreshing token...');

      var headers = {
        'accept': 'application/json, text/plain, */*',
      };

      if (_isLocalhost) {
        var token = _getRefreshToken();
        if (token) headers['X-Refresh-Token'] = token;
      }

      fetch(refreshUrl, {
        method: 'POST',
        headers: headers,
        credentials: 'include',
        mode: 'cors'
      })
        .then(function (response) {
          if (!response.ok) {
            return response.text().catch(function () { return 'Unknown error'; }).then(function (errText) {
              console.error('[TokenRefresh] Failed:', response.status, errText);
              resolve({ success: false, error: new Error('Refresh failed: ' + response.status) });
            });
          }
          return response.json().catch(function () { return {}; }).then(function (data) {
            console.log('[TokenRefresh] Token refreshed successfully');
            if (data.refresh_token) {
              _storeTokens(data.refresh_token, data.access_token);
            }
            resolve({ success: true, data: data });
          });
        })
        .catch(function (error) {
          console.error('[TokenRefresh] Error:', error);
          resolve({ success: false, error: error instanceof Error ? error : new Error(String(error)) });
        })
        .then(function () {
          // finally
          _isRefreshing = false;
          _refreshPromise = null;
        });
    });

    return _refreshPromise;
  }

  /**
   * Check if an HTTP status indicates an auth error
   */
  function _isAuthError(status) {
    return status === 401 || status === 403;
  }

  /**
   * Check if a WebSocket close code / reason indicates an auth error
   */
  function _isWsAuthError(code, reason) {
    if (code === 1008) return true;                      // Policy Violation
    if (code >= 4000 && code <= 4099) return true;       // Common auth error range

    var r = (reason || '').toLowerCase();
    return r.indexOf('auth') !== -1 ||
           r.indexOf('unauthorized') !== -1 ||
           r.indexOf('token') !== -1 ||
           r.indexOf('expired') !== -1 ||
           r.indexOf('invalid credential') !== -1;
  }

  // ==========================================
  // Constructor
  // ==========================================

  function TradiumDatafeed(options) {
    options = options || {};
    this.udfUrl         = options.udfUrl || '';
    this.wsUrl          = options.wsUrl || '';
    this._clientId      = options.clientId || '';
    this._groupId       = options.groupId || '';
    this.debug          = options.debug || false;
    this.barsPerRequest = options.barsPerRequest || 500;
    this.fetchOptions   = options.fetchOptions || {};

    // Token refresh config
    this.refreshTokenUrl = options.refreshTokenUrl || DEFAULT_REFRESH_TOKEN_URL;
    this.onAuthFailure   = options.onAuthFailure   || function () {};  // called when refresh fails (e.g. redirect to login)

    // Callbacks
    this.onConnect    = options.onConnect    || function () {};
    this.onDisconnect = options.onDisconnect || function () {};
    this.onError      = options.onError      || console.error;

    // WebSocket config
    this.maxReconnectAttempts = options.maxReconnectAttempts || 10;
    this.maxReconnectDelay    = options.maxReconnectDelay    || 5000;
    this.heartbeatInterval   = options.heartbeatInterval    || 5000;
    this.maxMissedPongs      = options.maxMissedPongs       || 3;

    // Internal state
    this.ws              = null;
    this.connectionState = 'disconnected';
    this.reconnectAttempts = 0;
    this.lastMessageTime = 0;
    this.lastPingTime    = 0;
    this.missedPongCount = 0;
    this.heartbeatTimer  = null;
    this.isActive        = false;
    this._wsAuthRefreshing = false;  // prevents auth-refresh loops on WS

    // Map<"symbol|resolution", Set<callback>>
    this._subscriptions = {};
    // Map<"symbol|resolution", callback> — the klinecharts-pro callback per pair
    this._proCallbacks  = {};
  }

  // ==========================================
  // Helpers
  // ==========================================

  TradiumDatafeed.prototype.log = function () {
    if (this.debug) console.log.apply(console, ['[TradiumDatafeed]'].concat([].slice.call(arguments)));
  };

  /**
   * Convert klinecharts-pro Period to UDF resolution string.
   *   { multiplier:1, timespan:'minute' } → '1'
   *   { multiplier:5, timespan:'minute' } → '5'
   *   { multiplier:1, timespan:'hour' }   → '60'
   *   { multiplier:4, timespan:'hour' }   → '240'
   *   { multiplier:1, timespan:'day' }    → '1D'
   *   { multiplier:1, timespan:'week' }   → '1W'
   *   { multiplier:1, timespan:'month' }  → '1M'
   *   { multiplier:1, timespan:'year' }   → '12M'
   */
  TradiumDatafeed.prototype.periodToResolution = function (period) {
    var m = period.multiplier;
    switch (period.timespan) {
      case 'tick':   return m + 'T';
      case 'second': return m + 'S';
      case 'minute': return String(m);
      case 'hour':   return String(m * 60);
      case 'day':    return m + 'D';
      case 'week':   return m + 'W';
      case 'month':  return m + 'M';
      case 'year':   return (m * 12) + 'M';
      default:       return '1D';
    }
  };

  /**
   * Convert UDF history response { s, t[], o[], h[], l[], c[], v[] }
   * to KLineData[] [{ timestamp, open, high, low, close, volume, turnover }]
   */
  TradiumDatafeed.prototype.udfBarsToCandles = function (data) {
    if (!data || data.s !== 'ok' || !data.t || !data.t.length) return [];
    var result = [];
    for (var i = 0; i < data.t.length; i++) {
      var ts = data.t[i];
      result.push({
        timestamp: ts > 1e12 ? ts : ts * 1000,
        open:   data.o[i],
        high:   data.h[i],
        low:    data.l[i],
        close:  data.c[i],
        volume: data.v ? data.v[i] : 0,
        turnover: 0
      });
    }
    return result;
  };

  // ==========================================
  // UDF REST API
  // ==========================================

  /**
   * Build the full URL for a UDF request (shared by _udfRequest internals).
   */
  TradiumDatafeed.prototype._buildUdfUrl = function (endpoint, params) {
    var url = this.udfUrl + endpoint + '?';
    var parts = [];
    if (this._clientId) parts.push('connection-user-id=' + encodeURIComponent(this._clientId));
    if (this._groupId)  parts.push('connection-group-id=' + encodeURIComponent(this._groupId));
    if (params) {
      Object.keys(params).forEach(function (k) {
        if (params[k] != null) parts.push(encodeURIComponent(k) + '=' + encodeURIComponent(params[k]));
      });
    }
    return url + parts.join('&');
  };

  /**
   * Make a UDF REST request.
   * On 401/403 → refresh token → retry once.
   * If refresh also fails, calls onAuthFailure and throws.
   */
  TradiumDatafeed.prototype._udfRequest = function (endpoint, params) {
    var self = this;
    var url  = this._buildUdfUrl(endpoint, params);
    var opts = Object.assign({ method: 'GET', credentials: 'include' }, this.fetchOptions);

    return fetch(url, opts).then(function (res) {
      if (_isAuthError(res.status)) {
        self.log('UDF got ' + res.status + ', attempting token refresh...');
        return _doTokenRefresh(self.refreshTokenUrl).then(function (result) {
          if (result.success) {
            self.log('Token refreshed, retrying UDF request...');
            // Retry with fresh credentials (cookie has been updated)
            return fetch(url, opts).then(function (retryRes) {
              if (!retryRes.ok) {
                if (_isAuthError(retryRes.status)) {
                  self.onAuthFailure({ source: 'udf', status: retryRes.status });
                }
                throw new Error('UDF request failed after token refresh: ' + retryRes.status);
              }
              return retryRes.json();
            });
          } else {
            self.log('Token refresh failed, giving up');
            self.onAuthFailure({ source: 'udf', status: res.status, error: result.error });
            throw new Error('UDF request failed: ' + res.status + ' (token refresh failed)');
          }
        });
      }
      if (!res.ok) throw new Error('UDF request failed: ' + res.status);
      return res.json();
    });
  };

  // ==========================================
  // klinecharts-pro Datafeed interface
  // ==========================================

  // ==========================================
  // Resolution ↔ Period helpers
  // ==========================================

  /**
   * Convert a UDF resolution string (e.g. "1", "60", "1D", "100T", "1S")
   * to a klinecharts-pro Period { multiplier, timespan, text }.
   */
  TradiumDatafeed.prototype.resolutionToPeriod = function (resolution) {
    if (!resolution) return null;
    var res = String(resolution);

    // Tick resolutions (e.g. "100T", "500T")
    if (/^\d+T$/i.test(res)) {
      var t = parseInt(res, 10);
      return { multiplier: t, timespan: 'tick', text: t + ' Ticks' };
    }
    // Second resolutions (e.g. "1S", "30S")
    if (/^\d+S$/i.test(res)) {
      var s = parseInt(res, 10);
      return { multiplier: s, timespan: 'second', text: s + (s === 1 ? ' Second' : ' Seconds') };
    }

    // Daily
    if (/^\d*D$/i.test(res)) {
      var d = parseInt(res, 10) || 1;
      return { multiplier: d, timespan: 'day', text: d === 1 ? 'Day' : d + ' Days' };
    }
    // Weekly
    if (/^\d*W$/i.test(res)) {
      var w = parseInt(res, 10) || 1;
      return { multiplier: w, timespan: 'week', text: w === 1 ? 'Week' : w + ' Weeks' };
    }
    // Monthly
    if (/^\d*M$/i.test(res)) {
      var mo = parseInt(res, 10) || 1;
      if (mo >= 12 && mo % 12 === 0) {
        var y = mo / 12;
        return { multiplier: y, timespan: 'year', text: y === 1 ? 'Year' : y + ' Years' };
      }
      return { multiplier: mo, timespan: 'month', text: mo === 1 ? 'Month' : mo + ' Months' };
    }

    // Intraday (plain number = minutes)
    var mins = parseInt(res, 10);
    if (isNaN(mins) || mins <= 0) return null;
    if (mins >= 60 && mins % 60 === 0) {
      var h = mins / 60;
      return { multiplier: h, timespan: 'hour', text: h + (h === 1 ? ' Hour' : ' Hours') };
    }
    return { multiplier: mins, timespan: 'minute', text: mins + (mins === 1 ? ' Min' : ' Mins') };
  };

  /**
   * Convert an array of UDF resolution strings to Period[].
   */
  TradiumDatafeed.prototype.resolutionsToPeriods = function (resolutions) {
    if (!Array.isArray(resolutions)) return [];
    var self = this;
    var result = [];
    resolutions.forEach(function (r) {
      var period = self.resolutionToPeriod(r);
      if (period) result.push(period);
    });
    return result;
  };

  /**
   * getSymbolInfo(symbol) → Promise<SymbolInfo>
   * Calls UDF /symbols endpoint to fetch full instrument details including
   * supported_resolutions, minTick, pipSize, pipValue, pointvalue, pricescale, etc.
   */
  TradiumDatafeed.prototype.getSymbolInfo = function (symbol) {
    var self = this;
    var ticker = symbol.ticker || symbol;

    return this._udfRequest('/symbols', { symbol: ticker, currencyCode: symbol.priceCurrency || 'USD' })
      .then(function (data) {
        if (!data || !data.ticker) return symbol;  // passthrough if bad response

        // Compute pricePrecision from pricescale if available
        var pricePrecision = symbol.pricePrecision || 2;
        if (data.pricescale) {
          // pricescale=100 → precision=2, pricescale=10000 → precision=4
          pricePrecision = Math.round(Math.log10(data.pricescale));
        }

        // Build enriched SymbolInfo
        var enriched = {
          ticker:           data.ticker || symbol.ticker,
          name:             data.description || data.name || symbol.name,
          shortName:        data.name || symbol.shortName,
          exchange:         data.exchange || symbol.exchange,
          market:           data.type || symbol.market,
          pricePrecision:   pricePrecision,
          volumePrecision:  symbol.volumePrecision || 0,
          priceCurrency:    data.currency_code || symbol.priceCurrency,
          type:             data.type || symbol.type,
          logo:             symbol.logo,
          // Tick / pip properties
          minTick:            data.minTick != null ? data.minTick : (data.minmov != null && data.pricescale ? data.minmov / data.pricescale : undefined),
          pipSize:            data.pipSize != null ? data.pipSize : undefined,
          pipValue:           data.pipValue != null ? data.pipValue : undefined,
          pointValue:         data.pointvalue != null ? data.pointvalue : undefined,
          // Supported resolutions (raw UDF strings + pre-converted Period[])
          supportedResolutions: data.supported_resolutions || undefined,
          supportedPeriods: data.supported_resolutions ? self.resolutionsToPeriods(data.supported_resolutions) : undefined
        };
        return enriched;
      })
      .catch(function (err) {
        self.onError('[UDF] getSymbolInfo error:', err);
        return symbol;  // fallback to original
      });
  };

  /**
   * searchSymbols(search?) → Promise<SymbolInfo[]>
   * Calls UDF /search endpoint
   */
  TradiumDatafeed.prototype.searchSymbols = function (search) {
    return this._udfRequest('/search', { query: search || '', limit: 30 })
      .then(function (results) {
        if (!Array.isArray(results)) return [];
        return results.map(function (item) {
          // ticker is always "exchange:symbol" (e.g. "CME:MES")
          var sym      = item.symbol || item.ticker || '';
          var exchange = item.exchange || '';
          var ticker   = sym.indexOf(':') !== -1 ? sym : (exchange ? exchange + ':' + sym : sym);
          var info = {
            ticker:     ticker,
            name:       item.description || item.name || '',
            shortName:  sym.split(':').pop() || sym,
            exchange:   exchange || ticker.split(':')[0] || '',
            market:     item.type || item.market || '',
            pricePrecision:  item.precision != null ? item.precision : (item.pricePrecision || 2),
            volumePrecision: item.volumePrecision || 0,
            type:       item.type || ''
          };
          // Pass through tick/pip properties when present in search response
          if (item.minTick != null)   info.minTick = item.minTick;
          if (item.pipSize != null)   info.pipSize = item.pipSize;
          if (item.pipValue != null)  info.pipValue = item.pipValue;
          if (item.pointvalue != null) info.pointValue = item.pointvalue;
          return info;
        });
      })
      .catch(function () { return []; });
  };

  /**
   * getHistoryKLineData(symbol, period, from, to) → Promise<KLineData[]>
   * Calls UDF /history endpoint
   */
  TradiumDatafeed.prototype.getHistoryKLineData = function (symbol, period, from, to) {
    var self = this;
    var resolution = this.periodToResolution(period);
    var ticker = symbol.ticker || symbol;

    return this._udfRequest('/history', {
      symbol:     ticker,
      resolution: resolution,
      from:       Math.floor(from / 1000),
      to:         Math.floor(to / 1000),
      countback:  this.barsPerRequest
    })
      .then(function (data) {
        return self.udfBarsToCandles(data);
      })
      .catch(function (err) {
        self.onError('[UDF] getHistoryKLineData error:', err);
        return [];
      });
  };

  /**
   * subscribe(symbol, period, callback) → void
   * Opens WebSocket (if not already) and subscribes to candle updates
   */
  TradiumDatafeed.prototype.subscribe = function (symbol, period, callback) {
    var resolution = this.periodToResolution(period);
    var ticker = symbol.ticker || symbol;
    var key = ticker + '|' + resolution;

    // Store the klinecharts-pro callback
    this._proCallbacks[key] = callback;

    // Register an internal WS callback that forwards to the pro callback
    var self = this;
    var wsCallback = function (_sym, _res, candle) {
      if (self._proCallbacks[key]) {
        self._proCallbacks[key]({
          timestamp: candle.timestamp,
          open:      candle.open,
          high:      candle.high,
          low:       candle.low,
          close:     candle.close,
          volume:    candle.volume || 0,
          turnover:  candle.turnover || 0
        });
      }
    };

    if (!this._subscriptions[key]) {
      this._subscriptions[key] = new Set();
    }
    this._subscriptions[key].add(wsCallback);

    // Connect if needed, then send subscribe message
    if (!this.isActive) this.connect();

    if (this.connectionState === 'connected') {
      this._sendCandlesMessage([ticker], [resolution], [], []);
    }

    this.log('subscribe:', ticker, resolution);
  };

  /**
   * unsubscribe(symbol, period) → void
   * Unsubscribes from candle updates
   */
  TradiumDatafeed.prototype.unsubscribe = function (symbol, period) {
    var resolution = this.periodToResolution(period);
    var ticker = symbol.ticker || symbol;
    var key = ticker + '|' + resolution;

    delete this._proCallbacks[key];
    delete this._subscriptions[key];

    if (this.connectionState === 'connected') {
      this._sendCandlesMessage([], [], [ticker], [resolution]);
    }

    this.log('unsubscribe:', ticker, resolution);
  };

  // ==========================================
  // WebSocket lifecycle
  // ==========================================

  TradiumDatafeed.prototype.connect = function () {
    this.isActive = true;
    this.reconnectAttempts = 0;
    this._connectWs(false);
  };

  TradiumDatafeed.prototype.disconnect = function () {
    this.isActive = false;
    this._closeConnection();
    this._subscriptions = {};
    this._proCallbacks  = {};
  };

  TradiumDatafeed.prototype._buildWsUrl = function () {
    if (!this.wsUrl) return null;
    if (this._clientId && this._groupId) {
      return this.wsUrl + '/' + this._clientId + '/' + this._groupId;
    }
    return this.wsUrl;
  };

  TradiumDatafeed.prototype._sendCandlesMessage = function (subSym, subRes, unsubSym, unsubRes) {
    return this._sendMessage({
      f:  FRAME_TYPES.CANDLES,
      s:  subSym,
      sr: subRes,
      u:  unsubSym,
      ur: unsubRes
    });
  };

  TradiumDatafeed.prototype._sendMessage = function (msg) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return false;
    try {
      this.ws.send(JSON.stringify(msg));
      this.log('Sent:', msg);
      return true;
    } catch (e) {
      this.onError('[WS] send failed:', e);
      return false;
    }
  };

  TradiumDatafeed.prototype._connectWs = function (isReconnect) {
    if (this.connectionState !== 'disconnected' || !this.isActive) return;
    var url = this._buildWsUrl();
    if (!url) { this.log('No WebSocket URL configured, skipping WS'); return; }

    this.connectionState = 'connecting';
    this.log('Connecting to', url);

    var self = this;
    var socket = new WebSocket(url);
    this.ws = socket;

    socket.onopen = function () {
      if (self.ws !== socket) return;
      self.connectionState = 'connected';
      self.reconnectAttempts = 0;
      self.lastMessageTime = Date.now();
      self.log('Connected');
      self._startHeartbeat();
      self._resubscribeAll();
      self.onConnect({ reconnect: isReconnect });
    };

    socket.onmessage = function (event) {
      if (self.ws !== socket) return;
      self.lastMessageTime = Date.now();
      if (event.data === 'pong') { self.log('Pong'); return; }
      try {
        var data = JSON.parse(event.data);
        self._handleMessage(data);
      } catch (e) {
        self.onError('[WS] parse error:', e);
      }
    };

    socket.onerror = function () {
      self.onError('[WS] WebSocket error');
    };

    socket.onclose = function (event) {
      if (self.ws !== socket) return;
      self.connectionState = 'disconnected';
      self.ws = null;
      self._stopHeartbeat();
      self.log('Disconnected:', event.code, event.reason);
      self.onDisconnect({ code: event.code, reason: event.reason });

      // If the close looks auth-related, refresh the token before reconnecting
      if (_isWsAuthError(event.code, event.reason) && !self._wsAuthRefreshing) {
        self._wsAuthRefreshing = true;
        self.log('WS closed due to auth error, refreshing token before reconnect...');
        _doTokenRefresh(self.refreshTokenUrl).then(function (result) {
          self._wsAuthRefreshing = false;
          if (result.success) {
            self.log('Token refreshed after WS auth error, reconnecting...');
            self.reconnectAttempts = 0; // reset since this is an auth issue, not a connectivity issue
            if (self.isActive && self.connectionState === 'disconnected') {
              self._connectWs(true);
            }
          } else {
            self.log('Token refresh failed after WS auth error');
            self.onAuthFailure({ source: 'websocket', code: event.code, reason: event.reason, error: result.error });
            // Still attempt normal reconnect schedule in case the server recovers
            self._scheduleReconnect();
          }
        });
      } else {
        self._scheduleReconnect();
      }
    };
  };

  TradiumDatafeed.prototype._closeConnection = function () {
    if (this.ws) { try { this.ws.close(); } catch (e) {} this.ws = null; }
    this.connectionState = 'disconnected';
    this._stopHeartbeat();
  };

  TradiumDatafeed.prototype._startHeartbeat = function () {
    this._stopHeartbeat();
    this.missedPongCount = 0;
    this.lastPingTime = 0;
    var self = this;

    this.heartbeatTimer = setInterval(function () {
      if (!self.isActive) return;
      if (self.lastPingTime > 0 && self.lastMessageTime < self.lastPingTime) {
        self.missedPongCount++;
        if (self.missedPongCount >= self.maxMissedPongs) {
          self.log('Connection stale, reconnecting...');
          self._closeConnection();
          self._connectWs(true);
          return;
        }
      } else {
        self.missedPongCount = 0;
      }
      if (self.ws && self.ws.readyState === WebSocket.OPEN) {
        self.lastPingTime = Date.now();
        try { self.ws.send('ping'); } catch (e) {}
      }
    }, this.heartbeatInterval);
  };

  TradiumDatafeed.prototype._stopHeartbeat = function () {
    if (this.heartbeatTimer) { clearInterval(this.heartbeatTimer); this.heartbeatTimer = null; }
  };

  TradiumDatafeed.prototype._scheduleReconnect = function () {
    if (!this.isActive) return;
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      this.onError('[WS] Max reconnect attempts reached');
      return;
    }
    var base = Math.min(1000 * Math.pow(2, this.reconnectAttempts), this.maxReconnectDelay);
    var delay = Math.floor(base * (Math.random() * 0.4 + 0.8));
    var self = this;
    this.log('Reconnecting in ' + delay + 'ms (attempt ' + (this.reconnectAttempts + 1) + ')');
    setTimeout(function () {
      if (self.isActive && self.connectionState === 'disconnected') {
        self.reconnectAttempts++;
        self._connectWs(true);
      }
    }, delay);
  };

  TradiumDatafeed.prototype._resubscribeAll = function () {
    var symbols = [], resolutions = [];
    Object.keys(this._subscriptions).forEach(function (key) {
      var parts = key.split('|');
      symbols.push(parts[0]);
      resolutions.push(parts[1]);
    });
    if (symbols.length) {
      this._sendCandlesMessage(symbols, resolutions, [], []);
      this.log('Resubscribed:', symbols, resolutions);
    }
  };

  // ==========================================
  // WebSocket message handling
  // ==========================================

  TradiumDatafeed.prototype._handleMessage = function (data) {
    if (data.f === FRAME_TYPES.ERROR) {
      this.onError('[WS] Server error:', data);

      // If the error frame indicates an auth issue, proactively refresh
      var errCode = data.code || data.status || 0;
      var errMsg  = String(data.message || data.reason || data.error || '').toLowerCase();
      if (_isAuthError(errCode) || errMsg.indexOf('auth') !== -1 ||
          errMsg.indexOf('token') !== -1 || errMsg.indexOf('expired') !== -1 ||
          errMsg.indexOf('unauthorized') !== -1) {
        this.log('WS error frame indicates auth issue, refreshing token...');
        var self = this;
        _doTokenRefresh(self.refreshTokenUrl).then(function (result) {
          if (result.success) {
            self.log('Token refreshed after WS auth error frame, reconnecting...');
            self._closeConnection();
            self.reconnectAttempts = 0;
            self._connectWs(true);
          } else {
            self.onAuthFailure({ source: 'websocket-frame', code: errCode, message: errMsg, error: result.error });
          }
        });
      }
      return;
    }
    if (data.f === FRAME_TYPES.CANDLES) {
      this._processCandleMessage(data);
      return;
    }
    this.log('Received frame:', data.f, data);
  };

  TradiumDatafeed.prototype._processCandleMessage = function (data) {
    var self = this;

    // Batch format: { f:5, c:[{ id, r, t, o, h, l, c, v }, ...] }
    if (data.c && Array.isArray(data.c)) {
      data.c.forEach(function (item) {
        self._emitCandle(String(item.id || item.s || ''), String(item.r || ''), item);
      });
      return;
    }

    // Single format: { f:5, id, r, t, o, h, l, c, v }
    var symbol = String(data.id || data.s || '');
    if (symbol) {
      this._emitCandle(symbol, String(data.r || ''), data);
    }
  };

  TradiumDatafeed.prototype._emitCandle = function (symbol, resolution, raw) {
    if (!raw) return;

    var candle;
    if (Array.isArray(raw)) {
      var ts = raw[0];
      candle = {
        timestamp: ts > 1e12 ? ts : ts * 1000,
        open: raw[1], high: raw[2], low: raw[3], close: raw[4],
        volume: raw[5] || 0, turnover: 0
      };
    } else {
      var ts = raw.t || raw.timestamp;
      candle = {
        timestamp: ts > 1e12 ? ts : ts * 1000,
        open:   raw.o != null ? raw.o : raw.open,
        high:   raw.h != null ? raw.h : raw.high,
        low:    raw.l != null ? raw.l : raw.low,
        close:  raw.c != null ? raw.c : raw.close,
        volume: (raw.v != null ? raw.v : raw.volume) || 0,
        turnover: 0
      };
    }

    var key = symbol + '|' + resolution;
    var callbacks = this._subscriptions[key];
    if (callbacks) {
      callbacks.forEach(function (cb) { cb(symbol, resolution, candle); });
    }
  };

  // ==========================================
  // Expose globally
  // ==========================================

  global.TradiumDatafeed = TradiumDatafeed;

})(typeof globalThis !== 'undefined' ? globalThis : typeof self !== 'undefined' ? self : this);
