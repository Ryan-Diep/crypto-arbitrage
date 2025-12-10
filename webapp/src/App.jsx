import React, { useState, useEffect, useRef } from 'react';
import { TrendingUp, RefreshCw, DollarSign, Activity, ArrowRightLeft, Radio, Eye, Zap, ChevronRight, Search } from 'lucide-react';

// Configuration for supported exchanges and their visual themes
const EXCHANGE_CONFIG = {
  COINBASE: { color: 'blue', label: 'Coinbase', text: 'text-blue-400', bg: 'bg-blue-500' },
  BINANCE: { color: 'yellow', label: 'Binance', text: 'text-yellow-400', bg: 'bg-yellow-500' },
  BYBIT: { color: 'orange', label: 'Bybit', text: 'text-orange-400', bg: 'bg-orange-500' },
  OKX: { color: 'white', label: 'OKX', text: 'text-white', bg: 'bg-slate-100' } // White/Black theme
};

const ORDERED_EXCHANGES = ['COINBASE', 'BINANCE', 'BYBIT', 'OKX'];

const ArbitrageDashboard = () => {
  const [opportunities, setOpportunities] = useState([]);
  const [stats, setStats] = useState({
    avgSpread: 0,
    maxSpread: 0,
    trackedSymbols: 0,
    activeOpportunities: 0
  });
  const [lastUpdate, setLastUpdate] = useState(new Date());
  const [isConnected, setIsConnected] = useState(false);
  const [activeTab, setActiveTab] = useState('opportunities');
  const [livePrices, setLivePrices] = useState([]);
  const [priceUpdates, setPriceUpdates] = useState([]);
  
  const [searchQuery, setSearchQuery] = useState('');

  const prevUpdatesRef = useRef([]);
  const [particles, setParticles] = useState([]);

  useEffect(() => {
    const newParticles = Array.from({ length: 50 }, (_, i) => ({
      id: i,
      x: Math.random() * 100,
      y: Math.random() * 100,
      size: Math.random() * 3 + 1,
      duration: Math.random() * 20 + 10,
      delay: Math.random() * 5
    }));
    setParticles(newParticles);
  }, []);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const oppResponse = await fetch('http://localhost:8081/api/opportunities');
        const oppData = await oppResponse.json();
        
        setOpportunities(oppData.opportunities || []);
        setStats({
          avgSpread: oppData.stats?.avgSpread ?? 0,
          maxSpread: oppData.stats?.maxSpread ?? 0,
          trackedSymbols: oppData.stats?.trackedSymbols ?? 0,
          activeOpportunities: oppData.stats?.activeOpportunities ?? 0
        });
        setLastUpdate(new Date(oppData.lastUpdate));
        setIsConnected(true);

        const pricesResponse = await fetch('http://localhost:8081/api/prices');
        const pricesData = await pricesResponse.json();
        
        if (pricesData.prices && Array.isArray(pricesData.prices)) {
          setLivePrices(pricesData.prices);
        }
        
        if (pricesData.updates && Array.isArray(pricesData.updates)) {
          if (pricesData.updates.length > 0) {
            setPriceUpdates(pricesData.updates);
            prevUpdatesRef.current = pricesData.updates;
          }
        }
        
      } catch (error) {
        console.error('Failed to fetch data:', error);
        setIsConnected(false);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 1000);

    return () => clearInterval(interval);
  }, []);

  const filteredPrices = livePrices.filter(item => 
    item.symbol.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const getAgeIndicator = (timestamp) => {
    const age = Date.now() - timestamp;
    if (age < 2000) return { symbol: '●', color: 'text-emerald-400', label: 'Fresh' };
    if (age < 5000) return { symbol: '◐', color: 'text-amber-400', label: 'Recent' };
    return { symbol: '○', color: 'text-slate-500', label: 'Older' };
  };

  const getExchangeStyle = (exchangeName) => {
    const config = EXCHANGE_CONFIG[exchangeName] || { color: 'gray', text: 'text-gray-400', bg: 'bg-gray-500' };
    return `${config.bg} bg-opacity-10 border border-${config.color}-500 border-opacity-30 ${config.text}`;
  };

  const getPriceStyle = (exchangeName) => {
    const config = EXCHANGE_CONFIG[exchangeName] || { text: 'text-gray-400' };
    return config.text;
  }

  const formatTime = (date) => {
    return date.toLocaleTimeString('en-US', { 
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  };

  const formatTimeWithMs = (timestamp) => {
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-US', { 
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    }) + '.' + date.getMilliseconds().toString().padStart(3, '0');
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-950 via-purple-950 to-slate-950 text-white overflow-hidden relative">
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        {particles.map((particle) => (
          <div
            key={particle.id}
            className="absolute rounded-full bg-purple-500 opacity-20"
            style={{
              left: `${particle.x}%`,
              top: `${particle.y}%`,
              width: `${particle.size}px`,
              height: `${particle.size}px`,
              animation: `float ${particle.duration}s ease-in-out infinite`,
              animationDelay: `${particle.delay}s`
            }}
          />
        ))}
      </div>

      <div className="absolute inset-0 opacity-30" style={{
        backgroundImage: 'linear-gradient(rgba(124,58,237,0.1) 1px, transparent 1px), linear-gradient(90deg, rgba(124,58,237,0.1) 1px, transparent 1px)',
        backgroundSize: '50px 50px'
      }} />

      <style>{`
        @keyframes float {
          0%, 100% { transform: translateY(0) translateX(0); }
          25% { transform: translateY(-20px) translateX(10px); }
          50% { transform: translateY(-10px) translateX(-10px); }
          75% { transform: translateY(-30px) translateX(5px); }
        }
      `}</style>

      <div className="relative z-10 p-6 max-w-[1800px] mx-auto">
        <div className="mb-8">
          <div className="flex items-center justify-between mb-8">
            <div className="flex items-center gap-4">
              <div className="relative">
                <div className="absolute inset-0 bg-gradient-to-r from-cyan-500 to-purple-600 rounded-2xl blur-xl opacity-50 animate-pulse" />
                <div className="relative bg-gradient-to-r from-cyan-500 to-purple-600 p-4 rounded-2xl">
                  <TrendingUp className="w-8 h-8" />
                </div>
              </div>
              <div>
                <h1 className="text-4xl font-bold bg-gradient-to-r from-cyan-400 via-purple-400 to-pink-400 bg-clip-text text-transparent mb-1">
                  Crypto Arbitrage Monitor
                </h1>
                <p className="text-slate-400 text-sm flex items-center gap-2">
                  <span className="w-2 h-2 rounded-full bg-cyan-400 animate-pulse" />
                  Real-time multi-exchange surveillance
                </p>
              </div>
            </div>
            <div className="flex items-center gap-3">
              <div className={`group relative px-6 py-3 rounded-xl backdrop-blur-xl transition-all duration-300 ${
                isConnected 
                  ? 'bg-emerald-500 bg-opacity-10 border border-emerald-500 border-opacity-20' 
                  : 'bg-rose-500 bg-opacity-10 border border-rose-500 border-opacity-20'
              }`}>
                <div className="flex items-center gap-3">
                  <div className="relative">
                    <div className={`w-3 h-3 rounded-full ${isConnected ? 'bg-emerald-400' : 'bg-rose-400'}`}>
                      <div className={`absolute inset-0 rounded-full ${isConnected ? 'bg-emerald-400' : 'bg-rose-400'} animate-ping`} />
                    </div>
                  </div>
                  <span className="font-semibold">{isConnected ? 'Live Feed Active' : 'Disconnected'}</span>
                </div>
              </div>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
            {[
              { icon: Activity, label: 'Active Opportunities', value: stats.activeOpportunities, color: 'cyan', suffix: '' },
              { icon: TrendingUp, label: 'Max Spread', value: Number(stats.maxSpread || 0).toFixed(2), color: 'emerald', suffix: '%' },
              { icon: DollarSign, label: 'Avg Spread', value: Number(stats.avgSpread || 0).toFixed(2), color: 'amber', suffix: '%' },
              { icon: ArrowRightLeft, label: 'Tracked Symbols', value: stats.trackedSymbols, color: 'purple', suffix: '' }
            ].map((stat, idx) => (
              <div 
                key={idx}
                className="group relative overflow-hidden"
              >
                <div className="absolute inset-0 bg-gradient-to-br from-slate-800 from-opacity-50 to-slate-900 to-opacity-50 rounded-2xl" />
                <div className="relative backdrop-blur-xl border border-slate-700 border-opacity-50 rounded-2xl p-6 transition-all duration-300">
                  <div className="flex items-start justify-between mb-4">
                    <div className={`p-3 rounded-xl bg-${stat.color}-500 bg-opacity-10 transition-transform duration-300`}>
                      <stat.icon className={`w-6 h-6 text-${stat.color}-400`} />
                    </div>
                    <ChevronRight className="w-5 h-5 text-slate-600 transition-all duration-300" />
                  </div>
                  <div className="space-y-2">
                    <div className="text-slate-400 text-sm font-medium">{stat.label}</div>
                    <div className={`text-4xl font-bold text-${stat.color}-400 transition-transform duration-300`}>
                      {stat.value}{stat.suffix}
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="flex gap-3 mb-6">
          {[
            { id: 'opportunities', label: 'Arbitrage Opportunities', icon: TrendingUp },
            { id: 'live-feed', label: 'Live Price Feed', icon: Radio, badge: priceUpdates.length }
          ].map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`group relative flex items-center gap-3 px-8 py-4 rounded-xl font-semibold transition-all duration-300 ${
                activeTab === tab.id
                  ? 'text-white'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              {activeTab === tab.id && (
                <>
                  <div className="absolute inset-0 bg-gradient-to-r from-cyan-500 to-purple-600 rounded-xl" />
                  <div className="absolute inset-0 bg-gradient-to-r from-cyan-500 to-purple-600 rounded-xl blur-xl opacity-50" />
                </>
              )}
              {activeTab !== tab.id && (
                <div className="absolute inset-0 bg-slate-800 bg-opacity-50 backdrop-blur-xl border border-slate-700 border-opacity-50 rounded-xl transition-all duration-300" />
              )}
              <tab.icon className="w-5 h-5 relative z-10" />
              <span className="relative z-10">{tab.label}</span>
              {tab.badge > 0 && (
                <span className="relative z-10 px-3 py-1 bg-emerald-500 text-slate-900 text-xs rounded-full font-bold animate-pulse">
                  {tab.badge}
                </span>
              )}
            </button>
          ))}
        </div>

        {activeTab === 'opportunities' && (
          <div className="relative overflow-hidden rounded-2xl">
            <div className="absolute inset-0 bg-gradient-to-br from-slate-800 from-opacity-50 to-slate-900 to-opacity-50" />
            <div className="relative backdrop-blur-xl border border-slate-700 border-opacity-50">
              <div className="relative overflow-hidden">
                <div className="absolute inset-0 bg-gradient-to-r from-cyan-600 via-purple-600 to-pink-600" />
                <div className="relative px-8 py-6">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <Zap className="w-6 h-6" />
                      <h2 className="text-2xl font-bold">Top Opportunities</h2>
                    </div>
                    <div className="flex items-center gap-3 text-sm backdrop-blur-xl bg-white bg-opacity-10 px-4 py-2 rounded-lg">
                      <RefreshCw className="w-4 h-4 animate-spin" />
                      <span>Updated {formatTime(lastUpdate)}</span>
                    </div>
                  </div>
                </div>
              </div>

              {opportunities.length === 0 ? (
                <div className="px-8 py-20 text-center">
                  <div className="inline-flex items-center justify-center w-20 h-20 rounded-2xl bg-gradient-to-br from-slate-700 from-opacity-50 to-slate-800 to-opacity-50 mb-6">
                    <Activity className="w-10 h-10 text-slate-500" />
                  </div>
                  <h3 className="text-2xl font-bold mb-3">No Opportunities Detected</h3>
                  <p className="text-slate-400 mb-4">Monitoring {stats.trackedSymbols} symbols across 4 exchanges...</p>
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b border-slate-700 border-opacity-50">
                        <th className="px-6 py-4 text-left text-xs font-bold text-slate-400 uppercase tracking-wider">Rank</th>
                        <th className="px-6 py-4 text-left text-xs font-bold text-slate-400 uppercase tracking-wider">Symbol</th>
                        <th className="px-6 py-4 text-left text-xs font-bold text-slate-400 uppercase tracking-wider">Buy From</th>
                        <th className="px-6 py-4 text-left text-xs font-bold text-slate-400 uppercase tracking-wider">Sell To</th>
                        <th className="px-6 py-4 text-right text-xs font-bold text-slate-400 uppercase tracking-wider">Buy Price</th>
                        <th className="px-6 py-4 text-right text-xs font-bold text-slate-400 uppercase tracking-wider">Sell Price</th>
                        <th className="px-6 py-4 text-right text-xs font-bold text-slate-400 uppercase tracking-wider">Spread</th>
                        <th className="px-6 py-4 text-right text-xs font-bold text-slate-400 uppercase tracking-wider">Profit</th>
                      </tr>
                    </thead>
                    <tbody>
                      {opportunities.map((opp, idx) => {
                        const ageInfo = getAgeIndicator(opp.timestamp);
                        return (
                          <tr 
                            key={idx} 
                            className="border-b border-slate-700 border-opacity-30 hover:bg-slate-700 hover:bg-opacity-20 transition-all duration-300 group"
                          >
                            <td className="px-6 py-5">
                              <div className="flex items-center gap-3">
                                <span className={`${ageInfo.color} text-xl`} title={ageInfo.label}>
                                  {ageInfo.symbol}
                                </span>
                                <span className="font-bold text-slate-300">#{idx + 1}</span>
                              </div>
                            </td>
                            <td className="px-6 py-5">
                              <span className="font-bold text-lg">{opp.symbol}</span>
                            </td>
                            <td className="px-6 py-5">
                              <span className={`px-4 py-2 rounded-lg text-sm font-semibold backdrop-blur-xl ${getExchangeStyle(opp.buyExchange)}`}>
                                {opp.buyExchange}
                              </span>
                            </td>
                            <td className="px-6 py-5">
                              <span className={`px-4 py-2 rounded-lg text-sm font-semibold backdrop-blur-xl ${getExchangeStyle(opp.sellExchange)}`}>
                                {opp.sellExchange}
                              </span>
                            </td>
                            <td className="px-6 py-5 text-right font-mono text-slate-300">
                              ${opp.buyPrice.toFixed(4)}
                            </td>
                            <td className="px-6 py-5 text-right font-mono text-slate-300">
                              ${opp.sellPrice.toFixed(4)}
                            </td>
                            <td className="px-6 py-5 text-right">
                              <div className="inline-flex items-center gap-2 px-4 py-2 rounded-lg backdrop-blur-xl">
                                <span className={`font-bold text-xl ${
                                  opp.spread >= 2 ? 'text-emerald-400' :
                                  opp.spread >= 1 ? 'text-amber-400' :
                                  'text-orange-400'
                                }`}>
                                  {opp.spread.toFixed(2)}%
                                </span>
                              </div>
                            </td>
                            <td className="px-6 py-5 text-right">
                              <span className="font-mono text-lg text-emerald-400 font-bold">
                                ${opp.priceDiff.toFixed(2)}
                              </span>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        )}

        {activeTab === 'live-feed' && (
          <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
            <div className="relative overflow-hidden rounded-2xl h-[700px] flex flex-col">
              <div className="absolute inset-0 bg-gradient-to-br from-slate-800 from-opacity-50 to-slate-900 to-opacity-50" />
              <div className="relative flex-1 backdrop-blur-xl border border-slate-700 border-opacity-50 flex flex-col">
                <div className="relative overflow-hidden shrink-0">
                  <div className="absolute inset-0 bg-gradient-to-r from-emerald-600 to-cyan-600" />
                  <div className="relative px-8 py-6">
                    <div className="flex items-center justify-between">
                      <h2 className="text-2xl font-bold flex items-center gap-3">
                        <Radio className="w-6 h-6 animate-pulse" />
                        Live Updates Stream
                      </h2>
                      <span className="text-sm backdrop-blur-xl bg-white bg-opacity-10 px-4 py-2 rounded-lg">
                        Last {Math.min(priceUpdates.length, 100)} updates
                      </span>
                    </div>
                  </div>
                </div>
                
                <div className="overflow-y-auto flex-1">
                  {priceUpdates.length === 0 ? (
                    <div className="flex items-center justify-center h-full">
                      <div className="text-center">
                        <Activity className="w-8 h-8 text-slate-500 animate-pulse mx-auto mb-4" />
                        <p className="text-slate-400">Waiting for price updates...</p>
                      </div>
                    </div>
                  ) : (
                    <div>
                      {priceUpdates.map((update, idx) => (
                        <div 
                          key={`${update.symbol}-${update.exchange}-${idx}`}
                          className="px-6 py-4 border-b border-slate-700 border-opacity-30 hover:bg-slate-700 hover:bg-opacity-20 transition-all duration-300"
                        >
                          <div className="flex items-center justify-between">
                            <div className="flex items-center gap-4">
                              <span className={`px-3 py-1.5 rounded-lg text-xs font-bold backdrop-blur-xl ${getExchangeStyle(update.exchange)}`}>
                                {update.exchange}
                              </span>
                              <span className="font-bold text-lg">{update.symbol}</span>
                            </div>
                            <div className="text-right">
                              <div className="font-mono text-emerald-400 font-bold text-lg">
                                ${typeof update.price === 'number' ? update.price.toFixed(4) : update.price}
                              </div>
                              <div className="text-xs text-slate-500 font-mono">
                                {formatTimeWithMs(update.timestamp)}
                              </div>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            </div>

            <div className="relative overflow-hidden rounded-2xl h-[700px] flex flex-col">
              <div className="absolute inset-0 bg-gradient-to-br from-slate-800 from-opacity-50 to-slate-900 to-opacity-50" />
              <div className="relative flex-1 backdrop-blur-xl border border-slate-700 border-opacity-50 flex flex-col">
                <div className="relative overflow-hidden shrink-0">
                  <div className="absolute inset-0 bg-gradient-to-r from-purple-600 to-pink-600" />
                  <div className="relative px-8 py-6">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-4">
                        <Eye className="w-6 h-6" />
                        <h2 className="text-2xl font-bold">Current Prices</h2>
                      </div>
                      
                      <div className="flex items-center gap-3">
                         <div className="relative">
                           <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-300" />
                           <input 
                             type="text" 
                             placeholder="Search..."
                             value={searchQuery}
                             onChange={(e) => setSearchQuery(e.target.value)}
                             className="pl-9 pr-4 py-2 bg-slate-900 bg-opacity-40 border border-slate-500 border-opacity-30 rounded-lg text-sm text-white placeholder-slate-300 focus:outline-none focus:border-white transition-all w-32 md:w-48"
                           />
                         </div>
                        <span className="text-sm backdrop-blur-xl bg-white bg-opacity-10 px-4 py-2 rounded-lg">
                          {filteredPrices.length}
                        </span>
                      </div>
                    </div>
                  </div>
                </div>
                
                <div className="overflow-y-auto flex-1">
                  {livePrices.length === 0 ? (
                    <div className="flex items-center justify-center h-full">
                      <div className="text-center">
                        <Activity className="w-8 h-8 text-slate-500 animate-pulse mx-auto mb-4" />
                        <p className="text-slate-400">No price data yet...</p>
                      </div>
                    </div>
                  ) : filteredPrices.length === 0 ? (
                    <div className="flex items-center justify-center h-full">
                      <p className="text-slate-400">No symbols match your search.</p>
                    </div>
                  ) : (
                    <table className="w-full">
                      <thead className="sticky top-0 backdrop-blur-xl bg-slate-800 bg-opacity-80 z-10">
                        <tr className="border-b border-slate-700 border-opacity-50">
                          <th className="px-6 py-4 text-left text-xs font-bold text-slate-400 uppercase">Symbol</th>
                          {ORDERED_EXCHANGES.map(ex => (
                            <th key={ex} className="px-6 py-4 text-right text-xs font-bold text-slate-400 uppercase">{EXCHANGE_CONFIG[ex]?.label || ex}</th>
                          ))}
                          <th className="px-6 py-4 text-right text-xs font-bold text-slate-400 uppercase">Spread</th>
                        </tr>
                      </thead>
                      <tbody>
                        {filteredPrices.map((item) => {
                          const hasSpread = item.spreadPercent !== undefined && item.spreadPercent !== null;
                          const bothFresh = item.bothFresh || false;
                          
                          return (
                            <tr 
                              key={item.symbol} 
                              className={`border-b border-slate-700 border-opacity-30 hover:bg-slate-700 hover:bg-opacity-20 transition-all duration-300 ${
                                bothFresh && hasSpread && item.spreadPercent >= 0.5 ? 'bg-emerald-500 bg-opacity-5' : ''
                              }`}
                            >
                              <td className="px-6 py-4 font-bold">
                                <div className="flex items-center gap-2">
                                  {item.symbol}
                                  {bothFresh && hasSpread && item.spreadPercent >= 0.5 && (
                                    <Zap className="w-4 h-4 text-emerald-400" />
                                  )}
                                </div>
                              </td>
                              
                              {ORDERED_EXCHANGES.map(ex => {
                                const price = item[ex];
                                const age = item[`${ex}_age`];
                                const exists = price !== undefined && price !== null;
                                const isFresh = exists && age <= 5000;

                                return (
                                  <td key={ex} className="px-6 py-4 text-right font-mono text-sm">
                                    {exists ? (
                                      <div className="flex flex-col items-end">
                                        <span className={isFresh ? getPriceStyle(ex) + " font-semibold" : "text-slate-600"}>
                                          ${price.toFixed(4)}
                                        </span>
                                        <span className="text-xs text-slate-600">{(age / 1000).toFixed(1)}s</span>
                                      </div>
                                    ) : (
                                      <span className="text-slate-700 text-xs">-</span>
                                    )}
                                  </td>
                                );
                              })}

                              <td className="px-6 py-4 text-right">
                                {hasSpread ? (
                                  <span className={`font-mono text-sm font-semibold ${
                                    bothFresh && item.spreadPercent >= 0.5 ? 'text-emerald-400' : 'text-slate-500'
                                  }`}>
                                    {item.spreadPercent.toFixed(2)}%
                                  </span>
                                ) : (
                                  <span className="text-slate-600">-</span>
                                )}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  )}
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default ArbitrageDashboard;