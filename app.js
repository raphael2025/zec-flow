const API = 'https://fapi.binance.com/fapi/v1';
const SPOT_API = 'https://api.binance.com/api/v3';
let SYMBOL = 'ZECUSDT';
let marketSocket, bookSocket, spotSocket;
let validSymbols = new Set(['ZECUSDT']);
const state = { symbolVersion: 0, price: null, mark: null, dayChange: 0, oi: null, oiSamples: [], oiHourly: [], currentCategory: null, trades: [], candles: [], obv: [], bids: new Map(), asks: new Map(), synced: false, depthBuffer: [], lastUpdate: null, selectedWindow: 300000, spotPrice: null, spotDayChange: 0, spotTrades: [], spotPriceSamples: [], perpPriceSamples: [], spotBids: new Map(), spotAsks: new Map(), spotSynced: false, spotDepthBuffer: [], spotLastUpdate: null, errors: {}, connected: { market: false, book: false, spot: false, spotBook: false } };
const $ = (id) => document.getElementById(id);
const fmt = (n, digits = 2) => Number.isFinite(n) ? n.toLocaleString('en-US', { maximumFractionDigits: digits, minimumFractionDigits: digits }) : '--';
const compact = (n) => Number.isFinite(n) ? Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 2 }).format(n) : '--';
const pct = (n) => `${n > 0 ? '+' : ''}${n.toFixed(2)}%`;
const now = () => Date.now();
const assetCode = () => SYMBOL.replace(/USDT$/, '');

async function get(path, params = {}) {
  const q = new URLSearchParams(params);
  const root = path.startsWith('futures/data/') ? 'https://fapi.binance.com' : API;
  const response = await fetch(`${root}/${path}?${q}`, { cache: 'no-store' });
  if (!response.ok) throw new Error(`${path}: HTTP ${response.status}`);
  return response.json();
}

function setConnection() {
  const ok = state.connected.market && state.connected.book && state.connected.spot && state.connected.spotBook;
  $('connection').className = `connection${ok ? '' : ' offline'}`;
  $('connection').innerHTML = `<i></i>${ok ? '实时连接' : Object.values(state.connected).some(Boolean) ? '部分连接' : '重连中'}`;
}

function connectMarket() {
  const streams = `${SYMBOL.toLowerCase()}@aggTrade/${SYMBOL.toLowerCase()}@kline_1m/${SYMBOL.toLowerCase()}@markPrice@1s`;
  const ws = new WebSocket(`wss://fstream.binance.com/market/stream?streams=${streams}`);
  marketSocket=ws;
  ws.onopen = () => { if(marketSocket!==ws)return;state.connected.market = true; setConnection(); };
  ws.onclose = () => { if(marketSocket!==ws)return;state.connected.market = false; setConnection(); setTimeout(()=>{if(marketSocket===ws)connectMarket();}, 2000); };
  ws.onerror = () => { if(marketSocket!==ws)return;state.connected.market = false; setConnection(); };
  ws.onmessage = ({ data }) => {
    if(marketSocket!==ws)return;
    const { stream, data: d } = JSON.parse(data);
    if (stream.endsWith('@aggTrade')) onTrade(d);
    else if (stream.endsWith('@kline_1m')) onKline(d.k);
    else if (stream.includes('@markPrice')) { state.mark = +d.p; renderPrice(); }
  };
}

function onTrade(t) {
  const q = +t.q, timestamp = +t.T;
  state.trades.push({ time: timestamp, qty: q, buy: !t.m, price: +t.p });
  pruneTrades();
  if (state.price == null) state.price = +t.p;
  recordPrice(state.perpPriceSamples, timestamp, +t.p);
  renderTradeStats();
  renderSpotComparison();
}

function connectSpot() {
  const streams=`${SYMBOL.toLowerCase()}@aggTrade/${SYMBOL.toLowerCase()}@depth@100ms`;
  const ws=new WebSocket(`wss://stream.binance.com:9443/stream?streams=${streams}`);
  spotSocket=ws;
  ws.onopen=()=>{if(spotSocket!==ws)return;state.connected.spot=true;setConnection();syncSpotBook();};
  ws.onclose=()=>{if(spotSocket!==ws)return;state.connected.spot=false;state.connected.spotBook=false;state.spotSynced=false;setConnection();setTimeout(()=>{if(spotSocket===ws)connectSpot();},2000);};
  ws.onerror=()=>{if(spotSocket!==ws)return;state.connected.spot=false;setConnection();};
  ws.onmessage=({data})=>{if(spotSocket!==ws)return;const {stream,data:d}=JSON.parse(data);if(stream.endsWith('@aggTrade'))onSpotTrade(d);else if(stream.includes('@depth'))onSpotDepth(d);};
}
function recordPrice(samples,time,price){const last=samples.at(-1);if(!last||time-last.time>=500){samples.push({time,price});while(samples.length&&time-samples[0].time>3600000)samples.shift();}else last.price=price;}
function onSpotTrade(t){const time=+t.T,price=+t.p;state.spotPrice=price;state.spotTrades.push({time,qty:+t.q,buy:!t.m,price});pruneSpotTrades();recordPrice(state.spotPriceSamples,time,price);renderSpotComparison();}
function pruneSpotTrades(){const cutoff=now()-300000;while(state.spotTrades.length&&state.spotTrades[0].time<cutoff)state.spotTrades.shift();}
async function getSpot(path,params={}){const q=new URLSearchParams(params),r=await fetch(`${SPOT_API}/${path}?${q}`,{cache:'no-store'});if(!r.ok)throw new Error(`Spot ${path}: HTTP ${r.status}`);return r.json();}
async function loadSymbols(){
  const status=$('symbol-status');
  try{
    const [futures,spot]=await Promise.all([get('exchangeInfo'),getSpot('exchangeInfo')]);
    const spotSet=new Set(spot.symbols.filter(x=>x.status==='TRADING'&&x.quoteAsset==='USDT'&&x.isSpotTradingAllowed!==false).map(x=>x.symbol));
    validSymbols=new Set(futures.symbols.filter(x=>x.status==='TRADING'&&x.contractType==='PERPETUAL'&&x.quoteAsset==='USDT'&&spotSet.has(x.symbol)).map(x=>x.symbol));
    const list=$('market-symbols');list.replaceChildren(...[...validSymbols].sort().map(symbol=>{const o=document.createElement('option');o.value=symbol;return o;}));
    status.textContent=`${validSymbols.size} 个现货+永续币种`;
  }catch(error){status.textContent='币种列表暂不可用';state.errors.symbols=error.message;}
}
function resetMarketState(){
  Object.assign(state,{price:null,mark:null,dayChange:0,oi:null,oiSamples:[],oiHourly:[],currentCategory:null,trades:[],candles:[],obv:[],bids:new Map(),asks:new Map(),synced:false,depthBuffer:[],lastUpdate:null,spotPrice:null,spotDayChange:0,spotTrades:[],spotPriceSamples:[],perpPriceSamples:[],spotBids:new Map(),spotAsks:new Map(),spotSynced:false,spotDepthBuffer:[],spotLastUpdate:null,errors:{},connected:{market:false,book:false,spot:false,spotBook:false}});
}
function setSymbol(){
  const requested=$('symbol-input').value.trim().toUpperCase();
  if(!/^[A-Z0-9]{3,24}USDT$/.test(requested)||!validSymbols.has(requested)){ $('symbol-status').textContent='请选择已上架的 USDT 现货+永续币种';return; }
  if(requested===SYMBOL)return;
  state.symbolVersion++;const oldSockets=[marketSocket,bookSocket,spotSocket];marketSocket=bookSocket=spotSocket=null;oldSockets.forEach(ws=>{try{ws?.close();}catch{}});
  SYMBOL=requested;$('symbol-status').textContent='拉取历史行情…';$('venue-symbol')?.replaceChildren(document.createTextNode(SYMBOL));document.title=`${SYMBOL} Flow · Binance 市场状态`;
  const asset=assetCode();$('base-heading').textContent=asset;$('asset-unit').textContent=asset;document.querySelectorAll('.asset-label').forEach(x=>x.textContent=asset);document.querySelectorAll('.symbol-label').forEach(x=>x.textContent=SYMBOL);
  resetMarketState();setConnection();renderPrice();renderHistoryStats();renderBook();renderSpotComparison();
  connectMarket();connectBook();connectSpot();
  loadCandles().then(loadOiHistory);loadOiMonth();refreshRest();syncSpotTicker();
}
async function syncSpotBook(){state.spotSynced=false;state.spotDepthBuffer=[];const symbol=SYMBOL,version=state.symbolVersion;try{
  const snap=await getSpot('depth',{symbol,limit:1000});
  if(version!==state.symbolVersion||symbol!==SYMBOL)return;
  state.spotBids=new Map(snap.bids.map(([p,q])=>[p,+q]));state.spotAsks=new Map(snap.asks.map(([p,q])=>[p,+q]));
  let i=state.spotDepthBuffer.findIndex(e=>e.U<=snap.lastUpdateId&&e.u>=snap.lastUpdateId+1);
  if(i<0)i=state.spotDepthBuffer.findIndex(e=>e.U<=snap.lastUpdateId+1&&e.u>=snap.lastUpdateId+1);
  if(i<0&&state.spotDepthBuffer.length)throw new Error('现货深度快照与实时增量未对齐');
  const buffered=state.spotDepthBuffer.slice(i<0?0:i);for(const e of buffered)if(e.u>snap.lastUpdateId)applySpotDepth(e);
  state.spotLastUpdate=buffered.at(-1)?.u??snap.lastUpdateId;state.spotSynced=true;state.connected.spotBook=true;state.spotDepthBuffer=[];state.errors.spotBook=null;setConnection();renderSpotComparison();
}catch(e){if(version!==state.symbolVersion)return;state.connected.spotBook=false;state.errors.spotBook=e.message;setConnection();$('source-status').textContent='现货盘口重同步';setTimeout(()=>{if(version===state.symbolVersion)syncSpotBook();},1500);}}
function onSpotDepth(e){if(!state.spotSynced){state.spotDepthBuffer.push(e);if(state.spotDepthBuffer.length>2500)state.spotDepthBuffer.shift();return;}if(e.u<=state.spotLastUpdate)return;if(e.U>state.spotLastUpdate+1){syncSpotBook();return;}applySpotDepth(e);state.spotLastUpdate=e.u;renderSpotComparison();}
function applySpotDepth(e){for(const [p,q] of e.b){if(+q===0)state.spotBids.delete(p);else state.spotBids.set(p,+q);}for(const [p,q] of e.a){if(+q===0)state.spotAsks.delete(p);else state.spotAsks.set(p,+q);}state.spotLastUpdate=e.u;}
async function syncSpotTicker(){const symbol=SYMBOL,version=state.symbolVersion;try{const t=await getSpot('ticker/24hr',{symbol});if(version!==state.symbolVersion)return;state.spotDayChange=+t.priceChangePercent;if(!state.spotPrice)state.spotPrice=+t.lastPrice;renderSpotComparison();}catch(e){if(version!==state.symbolVersion)return;state.errors.spotTicker=e.message;$('source-status').textContent='现货数据获取失败';}}
function sideImbalance(bids,asks,price){if(!price)return 0;let b=0,a=0;for(const[p,q]of bids)if(+p>=price*.995)b+=+p*q;for(const[p,q]of asks)if(+p<=price*1.005)a+=+p*q;return b+a?(b-a)/(b+a):0;}
function spotSpread(){const b=Math.max(...[...state.spotBids.keys()].map(Number)),a=Math.min(...[...state.spotAsks.keys()].map(Number)),m=(a+b)/2;return m?(a-b)/m*10000:0;}
function takerBuyShare(trades){const b=trades.filter(x=>x.buy).reduce((s,x)=>s+x.qty,0),a=trades.filter(x=>!x.buy).reduce((s,x)=>s+x.qty,0);return{share:b+a?b/(b+a)*100:50,volume:b+a};}
function priceAt(samples,time){let result=null;for(const x of samples){if(x.time<=time)result=x;else break;}return result;}
function firstDownCross(samples,base){if(!base)return null;for(const x of samples)if(x.time>=base.time&&x.price<=base.price*.9997)return x.time;return null;}
function renderSpotComparison(){
  const ss=takerBuyShare(state.spotTrades),ps=takerBuyShare(state.trades),si=sideImbalance(state.spotBids,state.spotAsks,state.spotPrice),pi=sideImbalance(state.bids,state.asks,state.price);
  if(state.spotPrice)$('spot-price').textContent=`$${fmt(state.spotPrice,2)}`;$('spot-change').textContent=pct(state.spotDayChange);$('spot-buy-share').textContent=`${ss.share.toFixed(1)}%`;$('perp-buy-share').textContent=`${ps.share.toFixed(1)}%`;
  $('perp-compare-price').textContent=state.price?`$${fmt(state.price,2)}`:'--';$('perp-change').textContent=pct(state.dayChange);$('spot-imbalance').textContent=`${si>0?'+':''}${(si*100).toFixed(1)}%`;$('spot-spread').textContent=state.spotSynced?`${spotSpread().toFixed(1)} bps`:'--';$('perp-imbalance').textContent=`${pi>0?'+':''}${(pi*100).toFixed(1)}%`;
  const oi=getWindowDelta();$('perp-oi-change').textContent=oi?`${oi.oiPct>0?'+':''}${oi.oiPct.toFixed(2)}%`:'基线中';
  renderSpotBook();
  if(!state.spotSynced||!state.synced){$('source-status').textContent='盘口同步中';return;}$('source-status').textContent='现货与永续已同步';
  const since=now()-300000,sb=priceAt(state.spotPriceSamples,since),pb=priceAt(state.perpPriceSamples,since);
  if(!sb||!pb||now()-Math.min(sb.time,pb.time)<60000){$('source-verdict').textContent='正在累积同步价格样本';$('source-detail').textContent='同一观察窗的现货与永续价格样本至少需要 1 分钟。';return;}
  const sr=(state.spotPrice/sb.price-1)*100,pr=(state.price/pb.price-1)*100,sc=firstDownCross(state.spotPriceSamples,sb),pc=firstDownCross(state.perpPriceSamples,pb);
  let title='现货与永续同步变化，来源暂不明确',detail=`近 5 分钟价格：现货 ${pct(sr)}，永续 ${pct(pr)}；主动买入：现货 ${ss.share.toFixed(1)}%，永续 ${ps.share.toFixed(1)}%。`;
  if(sc&&pc&&sc+3000<pc&&ss.share<ps.share){title=`现货卖压先出现 · 约早 ${Math.round((pc-sc)/1000)} 秒`;detail+='现货价格先下破同步基准，且现货主动卖压更强，符合现货先行特征。';}
  else if(sc&&pc&&pc+3000<sc&&ps.share<ss.share){title=`永续卖压先出现 · 约早 ${Math.round((sc-pc)/1000)} 秒`;detail+='永续价格先下破同步基准，且永续主动卖压更强，符合合约去杠杆先行特征。';}
  else if(ss.share<ps.share-5){title='现货主动卖压更强';detail+=`现货主动买入占比低 ${((ps.share-ss.share)).toFixed(1)} 个百分点，价格领先尚不清晰。`;}
  else if(ps.share<ss.share-5){title='永续主动卖压更强';detail+=`永续主动买入占比低 ${((ss.share-ps.share)).toFixed(1)} 个百分点，价格领先尚不清晰。`;}
  $('source-verdict').textContent=title;$('source-detail').textContent=detail;
}
function renderSpotBook(){
  if(!state.spotSynced){$('spot-book-mid').textContent='同步中';return;}
  const bids=[...state.spotBids].map(([p,q])=>[+p,q]).sort((a,b)=>b[0]-a[0]).slice(0,5),asks=[...state.spotAsks].map(([p,q])=>[+p,q]).sort((a,b)=>a[0]-b[0]).slice(0,5);
  const bid=bids[0]?.[0],ask=asks[0]?.[0],mid=(bid+ask)/2;if(!Number.isFinite(mid))return;
  const spread=(ask-bid)/mid*10000,maxBid=Math.max(...bids.map(x=>x[1]),1),maxAsk=Math.max(...asks.map(x=>x[1]),1);
  $('spot-book-mid').textContent=fmt(mid,2);$('spot-book-spread').textContent=`${spread.toFixed(1)} bps`;
  $('spot-asks').innerHTML=asks.slice().reverse().map(([p,q])=>bookRow(p,q,maxAsk,'ask')).join('');
  $('spot-bids').innerHTML=bids.map(([p,q])=>bookRow(p,q,maxBid,'bid')).join('');
}

function pruneTrades() {
  const cutoff = now() - 5 * 60000;
  while (state.trades.length && state.trades[0].time < cutoff) state.trades.shift();
}

function onKline(k) {
  const bar = { time: +k.t, close: +k.c, volume: +k.v, closed: k.x };
  const last = state.candles.at(-1);
  if (last && last.time === bar.time) state.candles[state.candles.length - 1] = bar;
  else if (bar.closed) state.candles.push(bar);
  if (bar.closed) {
    state.candles = state.candles.slice(-180);
    calcObv(); renderObv(); drawChart();
  }
  state.price = bar.close;
  renderPrice();
}

async function syncBook() {
  state.synced = false; state.depthBuffer = [];const symbol=SYMBOL,version=state.symbolVersion;
  try {
    const snapshot = await get('depth', { symbol, limit: 1000 });
    if (version!==state.symbolVersion||symbol!==SYMBOL) return;
    state.bids = new Map(snapshot.bids.map(([p, q]) => [p, +q]));
    state.asks = new Map(snapshot.asks.map(([p, q]) => [p, +q]));
    let update = state.depthBuffer.find((e) => e.U <= snapshot.lastUpdateId && e.u >= snapshot.lastUpdateId);
    if (!update) update = state.depthBuffer.find((e) => e.U <= snapshot.lastUpdateId + 1 && e.u >= snapshot.lastUpdateId + 1);
    if (!update && state.depthBuffer.length) throw new Error('盘口快照与增量未对齐');
    if (update) {
      const index = state.depthBuffer.indexOf(update);
      state.depthBuffer.slice(index).forEach(applyDepth);
    }
    state.synced = true; state.depthBuffer = []; state.lastUpdate = update?.u ?? snapshot.lastUpdateId;state.connected.book=true;setConnection();
    renderBook();
  } catch (error) {
    if(version!==state.symbolVersion)return;state.errors.book = error.message; state.synced = false; $('book-status').textContent = '重新同步';
    setTimeout(()=>{if(version===state.symbolVersion)syncBook();}, 1500);
  }
}

function connectBook() {
  const ws = new WebSocket(`wss://fstream.binance.com/public/stream?streams=${SYMBOL.toLowerCase()}@depth@100ms`);
  bookSocket=ws;
  ws.onopen = () => { state.connected.book = true; setConnection(); syncBook(); };
  ws.onclose = () => { if(bookSocket!==ws)return;state.connected.book = false; state.synced = false; setConnection(); setTimeout(()=>{if(bookSocket===ws)connectBook();}, 2000); };
  ws.onerror = () => { state.connected.book = false; setConnection(); };
  ws.onmessage = ({ data }) => {
    if(bookSocket!==ws)return;
    const { data: e } = JSON.parse(data);
    if (!state.synced) { state.depthBuffer.push(e); if (state.depthBuffer.length > 2500) state.depthBuffer.shift(); return; }
    if (e.pu !== state.lastUpdate) { syncBook(); return; }
    applyDepth(e); state.lastUpdate = e.u; renderBook();
  };
}

function applyDepth(event) {
  for (const [price, quantity] of event.b) { if (+quantity === 0) state.bids.delete(price); else state.bids.set(price, +quantity); }
  for (const [price, quantity] of event.a) { if (+quantity === 0) state.asks.delete(price); else state.asks.set(price, +quantity); }
  state.lastUpdate = event.u;
}

async function loadCandles() {
  const symbol=SYMBOL,version=state.symbolVersion;
  try {
    const data = await get('klines', { symbol, interval: '1m', limit: 180 });
    if(version!==state.symbolVersion)return;
    state.candles = data.slice(0, -1).map((r) => ({ time: +r[0], close: +r[4], volume: +r[5], closed: true }));
    const current = data.at(-1); if (current) state.price = +current[4];
    calcObv(); renderObv(); drawChart(); renderPrice();
  } catch (error) { if(version===state.symbolVersion)state.errors.candles = error.message; }
}

async function refreshRest() {
  const symbol=SYMBOL,version=state.symbolVersion;
  const results = await Promise.allSettled([
    get('openInterest', { symbol }), get('ticker/24hr', { symbol }), get('premiumIndex', { symbol })
  ]);
  if(version!==state.symbolVersion)return;
  if (results[0].status === 'fulfilled') {
    state.oi = { qty: +results[0].value.openInterest, time: now() };
    state.oiSamples.push({ ...state.oi, price: state.mark ?? state.price });
    state.oiSamples = state.oiSamples.filter((x) => now() - x.time <= 31 * 86400000).slice(-180000);
    $('oi').textContent = `${fmt(state.oi.qty, 0)} ${assetCode()}`;
    $('oi-usd').textContent = `$${compact(state.oi.qty * (state.mark ?? state.price ?? 0))}`;
    $('oi-age').textContent = '刚刚';
    renderVerdict(); drawChart();
  } else { state.errors.oi = results[0].reason.message; $('oi-age').textContent = '获取失败'; }
  if (results[1].status === 'fulfilled') {
    const t = results[1].value; state.dayChange = +t.priceChangePercent; if (!state.price) state.price = +t.lastPrice; renderPrice();
  }
  if (results[2].status === 'fulfilled') state.mark = +results[2].value.markPrice;
  renderPrice(); renderBook(); renderSpotComparison();
}

async function loadOiHistory() {
  const symbol=SYMBOL,version=state.symbolVersion;
  try {
    const history = await get('futures/data/openInterestHist', { symbol, period: '5m', limit: 30 });
    if(version!==state.symbolVersion)return;
    const candles = state.candles;
    if (!Array.isArray(history) || !history.length || !candles.length) return;
    const latest = history.map((x) => {
      const time = +x.timestamp;
      let candle = candles[0];
      for (const c of candles) { if (c.time <= time) candle = c; else break; }
      return { time, qty: +x.sumOpenInterest, price: candle.close };
    }).filter((x) => Number.isFinite(x.qty) && Number.isFinite(x.price));
    mergeOiSamples(latest);
  } catch (error) { if(version===state.symbolVersion)state.errors.oiHistory = error.message; }
}

async function loadOiMonth() {
  const symbol=SYMBOL,version=state.symbolVersion;
  try {
    const start = now() - 30 * 86400000;
    const [candles] = await Promise.all([get('klines', { symbol, interval: '1h', startTime: start, limit: 1000 })]);
    if(version!==state.symbolVersion)return;
    const candleRows = candles.map((r) => ({ time: +r[0], close: +r[4] }));
    const history = [];
    let endCursor = now();
    for (let page = 0; page < 4; page++) {
      const rows = await get('futures/data/openInterestHist', { symbol, period: '1h', startTime: start, endTime: endCursor, limit: 500 });
      if(version!==state.symbolVersion)return;
      if (!Array.isArray(rows) || !rows.length) break;
      history.push(...rows);
      const firstTime = +rows[0].timestamp;
      if (rows.length < 500 || firstTime <= start || firstTime >= endCursor) break;
      endCursor = firstTime - 1;
    }
    const samples = history.sort((a,b) => +a.timestamp - +b.timestamp).map((row) => {
      const time = +row.timestamp;
      let candle = candleRows[0];
      for (const c of candleRows) { if (c.time <= time) candle = c; else break; }
      return { time, qty: +row.sumOpenInterest, price: candle?.close };
    }).filter((x) => Number.isFinite(x.qty) && Number.isFinite(x.price));
    state.oiHourly = samples;
    $('symbol-status').textContent=`${SYMBOL} · 已载入 ${samples.length} 个小时 OI 点`;
    renderHistoryStats();
    mergeOiSamples(samples);
  } catch (error) { if(version!==state.symbolVersion)return;state.errors.oiMonth = error.message; $('oi-age').textContent = '历史数据加载失败';$('symbol-status').textContent=`${SYMBOL} · 历史数据加载失败`; }
}

function mergeOiSamples(incoming) {
  const byTime = new Map(state.oiSamples.map((x) => [x.time, x]));
  for (const sample of incoming) byTime.set(sample.time, { ...byTime.get(sample.time), ...sample });
  state.oiSamples = [...byTime.values()].sort((a,b) => a.time-b.time).slice(-180000);
  renderVerdict(); drawChart();
}

function historicalCategory(a,b) {
  const p = (b.price / a.price - 1) * 100;
  const oi = (b.qty / a.qty - 1) * 100;
  const threshold = Math.max(.03, 10 / Math.max(a.qty, 1) * 100);
  const priceDirection = p > .03 ? 1 : p < -.03 ? -1 : 0;
  const oiDirection = oi > threshold ? 1 : oi < -threshold ? -1 : 0;
  if (!priceDirection || !oiDirection) return null;
  if (priceDirection > 0 && oiDirection > 0) return 'long-add';
  if (priceDirection < 0 && oiDirection > 0) return 'short-add';
  if (priceDirection > 0 && oiDirection < 0) return 'short-cover';
  return 'long-close';
}

function renderHistoryStats() {
  const categories=[['long-add','多头增仓'],['short-add','空头增仓'],['short-cover','空头回补'],['long-close','多头平仓']];
  const horizons=[1,4,24];
  const data=state.oiHourly;
  if(data.length<30){$('probability-rows').innerHTML='<tr><td colspan="4">历史样本不足，暂不计算。</td></tr>';return;}
  const rows=categories.map(([key,label])=>{
    const counts=Object.fromEntries(horizons.map(h=>[h,{down:0,up:0,flat:0,n:0}]));
    for(let i=1;i<data.length;i++){
      if(data[i].time-data[i-1].time>90*60000||historicalCategory(data[i-1],data[i])!==key)continue;
      for(const h of horizons){
        const targetTime=data[i].time+h*3600000;
        let j=i+h;
        while(j<data.length&&data[j].time<targetTime-30*60000)j++;
        if(j>=data.length||Math.abs(data[j].time-targetTime)>90*60000||!data[i].price)continue;
        const move=(data[j].price/data[i].price-1)*100,c=counts[h];
        if(move<-.05)c.down++;else if(move>.05)c.up++;else c.flat++;
        c.n++;
      }
    }
    const cells=horizons.map(h=>{
      const c=counts[h];if(!c.n)return '<td><span class="prob-no-data">样本不足</span></td>';
      return `<td><strong class="prob-down">↓ ${Math.round(c.down/c.n*100)}%</strong><small>↑ ${Math.round(c.up/c.n*100)}% · 横 ${Math.round(c.flat/c.n*100)}% · n=${c.n}</small></td>`;
    }).join('');
    return `<tr data-category="${key}" class="${state.currentCategory===key?'current-state':''}"><th scope="row">${label}${state.currentCategory===key?'<i>当前</i>':''}</th>${cells}</tr>`;
  });
  $('probability-rows').innerHTML=rows.join('');
}

function getWindowDelta() {
  if (!state.oiSamples.length) return null;
  const current = state.oiSamples.at(-1), cutoff = current.time - state.selectedWindow;
  let base = null, distance = Infinity;
  for (const sample of state.oiSamples) {
    const nextDistance = Math.abs(sample.time - cutoff);
    if (sample.time < current.time && nextDistance < distance) { base = sample; distance = nextDistance; }
  }
  const duration = base ? current.time - base.time : 0;
  if (!base || duration < state.selectedWindow * .65 || duration > state.selectedWindow * 1.6 || !base.price || !current.price) return null;
  return { oi: current.qty - base.qty, oiPct: (current.qty / base.qty - 1) * 100, price: ((current.price / base.price) - 1) * 100, duration };
}

function renderVerdict() {
  const d = getWindowDelta();
  const labels = { 60000:'1 分钟', 300000:'5 分钟', 900000:'15 分钟', 3600000:'1 小时', 86400000:'24 小时', 604800000:'7 天', 2592000000:'30 天' };
  $('window-label').textContent = `近 ${labels[state.selectedWindow]}`;
  $('chart-left').textContent = state.oiSamples.length ? new Date(state.oiSamples[0].time).toLocaleTimeString('zh-CN', { hour12: false }) : '开始采样';
  if (!d) { state.currentCategory=null; $('verdict-title').textContent = '正在收集 OI 基线'; const required=Math.ceil(state.selectedWindow/1000);$('verdict-detail').textContent=state.oiHourly.length>=30?`近 30 天历史已载入；当前观察窗需要至少 ${required} 秒连续 OI 样本，正在等待可比较的实时基线。`:`需要至少 ${required} 秒的连续观测，当前已有约 ${Math.floor((now() - (state.oiSamples[0]?.time ?? now())) / 1000)} 秒数据。`; $('confidence').textContent = '等待'; renderHistoryStats(); return; }
  $('oi-change').textContent = `${d.oi >= 0 ? '+' : ''}${fmt(d.oi, 0)} ${assetCode()} · ${pct(d.oiPct)}`;
  $('oi-change').className = d.oi >= 0 ? 'positive' : 'negative';
  const buyVol = state.trades.filter((t) => t.buy).reduce((s, t) => s + t.qty, 0);
  const sellVol = state.trades.filter((t) => !t.buy).reduce((s, t) => s + t.qty, 0);
  const takerBuyPct = (buyVol + sellVol) ? buyVol / (buyVol + sellVol) * 100 : 50;
  const obvDelta = state.obv.length > 1 ? state.obv.at(-1) - state.obv[Math.max(0, state.obv.length - 6)] : 0;
  const bookImb = getBookImbalance();
  const thresholdOi = Math.max(.03, 10 / Math.max(state.oi?.qty ?? 1, 1) * 100);
  const pUp = d.price > .03, pDown = d.price < -.03, oiUp = d.oiPct > thresholdOi, oiDown = d.oiPct < -thresholdOi;
  state.currentCategory = pUp&&oiUp?'long-add':pDown&&oiUp?'short-add':pUp&&oiDown?'short-cover':pDown&&oiDown?'long-close':null;
  let title = '方向不明 · OI 变化幅度不足', detail = `价格 ${pct(d.price)}，OI ${pct(d.oiPct)}。变化接近噪声阈值，等待更清晰的变化。`, color = 'neutral';
  if (pUp && oiUp) { title = '多头增仓倾向（推断）'; detail = `价格上升 ${pct(d.price)} 且 OI 增加 ${pct(d.oiPct)}。若主动买入、OBV 同步走强，增仓判断更可信。`; color = 'up'; }
  else if (pDown && oiUp) { title = '空头增仓倾向（推断）'; detail = `价格下降 ${pct(d.price)} 且 OI 增加 ${pct(d.oiPct)}。若主动卖出、OBV 同步走弱，增仓判断更可信。`; color = 'down'; }
  else if (pUp && oiDown) { title = '空头回补倾向（推断）'; detail = `价格上升 ${pct(d.price)} 而 OI 减少 ${pct(d.oiPct)}，符合空头平仓推动上涨的特征。`; color = 'up'; }
  else if (pDown && oiDown) { title = '多头平仓倾向（推断）'; detail = `价格下降 ${pct(d.price)} 而 OI 减少 ${pct(d.oiPct)}，符合多头减仓或多头清算的特征。`; color = 'down'; }
  const confirmations = [color === 'up' ? takerBuyPct > 53 : color === 'down' ? takerBuyPct < 47 : false, color === 'up' ? obvDelta > 0 : color === 'down' ? obvDelta < 0 : false, color === 'up' ? bookImb > .08 : color === 'down' ? bookImb < -.08 : false].filter(Boolean).length;
  const score = color === 'neutral' ? 0 : Math.round(35 + confirmations * 20);
  $('verdict-icon').textContent = color === 'up' ? '↗' : color === 'down' ? '↘' : '···';
  $('verdict-icon').style.color = color === 'down' ? 'var(--red)' : color === 'up' ? 'var(--mint)' : 'var(--amber)';
  $('verdict-title').textContent = title; $('verdict-detail').textContent = `${detail} 当前主动买入占比 ${takerBuyPct.toFixed(1)}%，盘口不平衡 ${fmt(bookImb * 100, 1)}%。`;
  $('confidence').textContent = color === 'neutral' ? '偏低' : `${score}%`;
  $('confidence').style.color = color === 'down' ? 'var(--red)' : 'var(--mint)'; $('confidence-bar').style.width = `${score}%`; $('confidence-bar').style.background = color === 'down' ? 'var(--red)' : 'var(--mint)';
  drawChart();
  renderHistoryStats();
}

function renderPrice() {
  if (state.price) $('last-price').textContent = `$${fmt(state.price, 2)}`;
  if (state.mark) $('mark-price').textContent = `$${fmt(state.mark, 2)}`;
  $('day-change').textContent = pct(state.dayChange);
  $('day-change').className = `change ${state.dayChange < 0 ? 'negative' : ''}`;
}

function renderTradeStats() {
  pruneTrades();
  const buy = state.trades.filter((x) => x.buy).reduce((s, x) => s + x.qty, 0);
  const sell = state.trades.filter((x) => !x.buy).reduce((s, x) => s + x.qty, 0);
  const share = buy + sell ? buy / (buy + sell) * 100 : 50;
  $('buy-share').textContent = `${share.toFixed(1)}%`;
  $('buy-share').className = share >= 50 ? 'positive' : 'negative';
  $('buy-meter').style.width = `${share}%`;
  $('taker-vol').textContent = `${fmt(buy + sell, 1)} ${assetCode()}`;
  renderVerdict();
}

function calcObv() {
  let obv = 0; state.obv = [];
  for (let i = 0; i < state.candles.length; i++) {
    if (i > 0) obv += Math.sign(state.candles[i].close - state.candles[i - 1].close) * state.candles[i].volume;
    state.obv.push(obv);
  }
}

function renderObv() {
  if (state.obv.length < 6) return;
  const delta = state.obv.at(-1) - state.obv.at(-6);
  $('obv-state').textContent = delta > 0 ? '净流入' : delta < 0 ? '净流出' : '持平';
  $('obv-state').className = delta > 0 ? 'positive' : delta < 0 ? 'negative' : '';
  $('obv-delta').textContent = `${delta > 0 ? '+' : ''}${fmt(delta, 0)} ${assetCode()} · 近 5 根`;
}

function getBookImbalance() {
  if (!state.synced || !state.price) return 0;
  const mid = state.price, lo = mid * .995, hi = mid * 1.005;
  let bids = 0, asks = 0;
  for (const [p, q] of state.bids) if (+p >= lo) bids += +p * q;
  for (const [p, q] of state.asks) if (+p <= hi) asks += +p * q;
  return (bids + asks) ? (bids - asks) / (bids + asks) : 0;
}

function renderBook() {
  if (!state.synced) { $('book-status').textContent = '同步中'; $('book-state').textContent = '正在同步深度'; return; }
  const bids = [...state.bids].map(([p, q]) => [+p, q]).sort((a,b) => b[0]-a[0]).slice(0, 8);
  const asks = [...state.asks].map(([p, q]) => [+p, q]).sort((a,b) => a[0]-b[0]).slice(0, 8);
  const bestBid = bids[0]?.[0], bestAsk = asks[0]?.[0], mid = (bestBid + bestAsk) / 2;
  if (!Number.isFinite(mid)) return;
  const spread = (bestAsk - bestBid) / mid * 10000;
  $('book-status').textContent = '已同步'; $('book-mid').textContent = fmt(mid, 2); $('book-spread').textContent = `${spread.toFixed(1)} bps`; $('spread').textContent = `价差 ${spread.toFixed(1)} bps`;
  const imb = getBookImbalance(); $('book-ratio').textContent = `${imb > 0 ? '+' : ''}${(imb * 100).toFixed(1)}%`;
  $('book-ratio').className = imb > .03 ? 'positive' : imb < -.03 ? 'negative' : '';
  $('book-state').textContent = imb > .03 ? '买盘深度较厚' : imb < -.03 ? '卖盘深度较厚' : '买卖深度接近';
  const maxAsk = Math.max(...asks.map((x) => x[1]), 1), maxBid = Math.max(...bids.map((x) => x[1]), 1);
  $('asks').innerHTML = asks.slice(0,5).reverse().map(([p,q]) => bookRow(p,q,maxAsk,'ask')).join('');
  $('bids').innerHTML = bids.slice(0,5).map(([p,q]) => bookRow(p,q,maxBid,'bid')).join('');
}
function bookRow(p,q,max,side) { const width = Math.min(100,q/max*100); return `<div class="book-row ${side}"><i class="depth-bg" style="width:${width}%"></i><span class="price">${fmt(p,2)}</span><span>${fmt(q,1)}</span><span>${width.toFixed(0)}%</span></div>`; }

function drawChart() {
  const canvas = $('chart'), rect = canvas.getBoundingClientRect(), dpr = devicePixelRatio || 1;
  if (!rect.width) return;
  canvas.width = rect.width * dpr; canvas.height = rect.height * dpr;
  const ctx = canvas.getContext('2d'); ctx.scale(dpr,dpr);
  const W=rect.width,H=rect.height,pad={l:4,r:34,t:12,b:12},w=W-pad.l-pad.r,h=H-pad.t-pad.b;
  ctx.clearRect(0,0,W,H);
  let samples=state.oiSamples.filter(x=>now()-x.time<=Math.max(state.selectedWindow*2,600000));
  if(samples.length>2400){const stride=Math.ceil(samples.length/2400);samples=samples.filter((_,i)=>i%stride===0||i===samples.length-1);}
  if(samples.length<2){ctx.fillStyle='#687880';ctx.font='11px DM Mono';ctx.fillText('累积实时 OI 快照后显示图表',10,H/2);return;}
  const minT=samples[0].time,maxT=samples.at(-1).time,prices=samples.map(x=>x.price).filter(Number.isFinite),baseOi=samples[0].qty;
  let minP=Math.min(...prices),maxP=Math.max(...prices);if(maxP-minP<.01){minP-=.01;maxP+=.01;}
  const oiVals=samples.map(x=>(x.qty/baseOi-1)*100),minO=Math.min(...oiVals),maxO=Math.max(...oiVals),oiSpan=Math.max(.02,maxO-minO);
  ctx.strokeStyle='#253139';ctx.lineWidth=1;
  for(let i=0;i<4;i++){let y=pad.t+h*i/3;ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(W-pad.r,y);ctx.stroke();}
  ctx.font='9px DM Mono';ctx.fillStyle='#71818a';ctx.textAlign='right';
  [maxP,(maxP+minP)/2,minP].forEach((v,i)=>ctx.fillText(v.toFixed(2),W-2,pad.t+h*i/2+3));
  ctx.textAlign='left';ctx.fillStyle='#7ca9ff';
  [maxO,(maxO+minO)/2,minO].forEach((v,i)=>ctx.fillText(`${v>=0?'+':''}${v.toFixed(2)}%`,2,pad.t+h*i/2+3));
  const drawLine=(vals,min,max,color)=>{ctx.beginPath();vals.forEach((v,i)=>{const x=pad.l+(samples[i].time-minT)/Math.max(1,maxT-minT)*w,y=pad.t+h-(v-min)/(max-min)*h;i?ctx.lineTo(x,y):ctx.moveTo(x,y);});ctx.strokeStyle=color;ctx.lineWidth=1.8;ctx.stroke();};
  drawLine(prices,minP,maxP,'#52e0ae'); drawLine(oiVals,minO,minO+oiSpan,'#7ca9ff');
}

document.querySelectorAll('.periods button').forEach((button)=>button.addEventListener('click',()=>{
  document.querySelector('.periods .selected')?.classList.remove('selected');button.classList.add('selected');state.selectedWindow=+button.dataset.window;renderVerdict();
}));
setInterval(()=>{$('clock').textContent=`${new Date().toISOString().slice(11,19)} UTC`;if(state.oi) $('oi-age').textContent=`${Math.floor((now()-state.oi.time)/1000)} 秒前`;},1000);
window.addEventListener('resize',drawChart);
$('load-symbol').addEventListener('click',setSymbol);$('symbol-input').addEventListener('keydown',(e)=>{if(e.key==='Enter')setSymbol();});
loadSymbols();connectMarket();connectBook();connectSpot();loadCandles().then(loadOiHistory);loadOiMonth();refreshRest();syncSpotTicker();setInterval(refreshRest,15000);setInterval(syncSpotTicker,15000);setInterval(renderTradeStats,5000);
