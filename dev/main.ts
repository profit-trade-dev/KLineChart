import { init, dispose } from '../src/index'
import type { SymbolInfo } from '../src/common/SymbolInfo'
import type { Period, PeriodType } from '../src/common/Period'
import type { DataLoader, DataLoaderGetBarsParams, DataLoaderSubscribeBarParams, DataLoaderUnsubscribeBarParams } from '../src/common/DataLoader'
import type { KLineData } from '../src/common/Data'
import { styles } from './config'

declare const TradiumDatafeed: any

const UDF_BASE = 'https://prod-market-data.tradesea.ai/v1'
const WS_BASE = 'wss://prod-market-data.tradesea.ai/v1/wss'
const CLIENT_ID = 'ZWULU59974_PR2HEYLEMVZWKYL4GN6DE7CLKVIFQTZYGY4DKMK7JVHDEV2PG5BVGS2OGZCEON2CKM'
const GROUP_ID = 'c9c5a655de36ef6906a6ca706165fac380015bdf20dbb4bfec3601748f63f6a5'
const DEFAULT_SYMBOL: SymbolInfo = { ticker: 'CME:MES', pricePrecision: 2, volumePrecision: 0 }
const DEFAULT_PERIOD: Period = { type: 'day', span: 1 }

const feed = new TradiumDatafeed({
  udfUrl: UDF_BASE,
  wsUrl: WS_BASE,
  clientId: CLIENT_ID,
  groupId: GROUP_ID,
  debug: true,
  barsPerRequest: 500,
  onAuthFailure: (info: any) => console.warn('[Auth] failure:', info)
})

function periodToFeedPeriod (p: Period): { multiplier: number, timespan: string } {
  return { multiplier: p.span, timespan: p.type }
}

const dataLoader: DataLoader = {
  getBars (params: DataLoaderGetBarsParams) {
    const feedPeriod = periodToFeedPeriod(params.period)
    const now = Date.now()
    const yearMs = 365 * 24 * 60 * 60 * 1000

    let from: number
    let to: number
    if (params.type === 'forward' && params.timestamp != null) {
      from = params.timestamp - yearMs
      to = params.timestamp
    } else if (params.type === 'backward' && params.timestamp != null) {
      from = params.timestamp
      to = params.timestamp + yearMs
    } else {
      from = now - yearMs
      to = now
    }

    feed.getHistoryKLineData(params.symbol, feedPeriod, from, to)
      .then((bars: KLineData[]) => {
        const more = params.type === 'init' ? { forward: bars.length > 0 } : false
        params.callback(bars, more)
      })
      .catch((err: any) => {
        console.error('[DataLoader] getBars error:', err)
        params.callback([])
      })
  },

  subscribeBar (params: DataLoaderSubscribeBarParams) {
    const feedPeriod = periodToFeedPeriod(params.period)
    feed.subscribe(params.symbol, feedPeriod, (bar: KLineData) => {
      params.callback(bar)
    })
  },

  unsubscribeBar (params: DataLoaderUnsubscribeBarParams) {
    const feedPeriod = periodToFeedPeriod(params.period)
    feed.unsubscribe(params.symbol, feedPeriod)
  }
}

let chart: ReturnType<typeof init> = null

function createChart (): void {
  chart = init('chart',{
    zoomAnchor:'last_bar',
    layout: [{
      type: 'candle',
      options: {
        axis: {
          position: 'right',
          reverse: false,
          // We use createTicks to force the 0.25 step
          createTicks: (params) => {
            console.log(params);

            return params.defaultTicks;
            // const { range, from, to } = params;
            // const ticks = [];
            
            // // Calculate the first tick value that is a multiple of minTick
            // let currentVal = Math.ceil(from / minTick) * minTick;
  
            // // Loop through the visible range and create tick objects
            // while (currentVal <= to) {
            //   ticks.push({
            //     value: currentVal,
            //     text: currentVal.toFixed(2), // Format to 2 decimal places
            //     // Note: 'coord' is usually calculated internally by the library 
            //     // based on value, but you can provide it if doing custom projection.
            //   });
            //   currentVal += minTick;
            // }
  
            // Optimization: If there are too many ticks (zoomed out), 
            // you might want to skip every Nth tick to prevent overlap.
            // return ticks;
          }
        }
      }
    }]
  });

  if (chart === null) return 
  chart.setDataLoader(dataLoader)
  chart.setSymbol(DEFAULT_SYMBOL)
  chart.setPeriod(DEFAULT_PERIOD)
  chart.setStyles(styles)
}
// --- Period mapping ---

const RESOLUTION_TO_PERIOD: Record<string, Period> = {
  '1': { type: 'minute', span: 1 },
  '5': { type: 'minute', span: 5 },
  '15': { type: 'minute', span: 15 },
  '60': { type: 'hour', span: 1 },
  '240': { type: 'hour', span: 4 },
  '1D': { type: 'day', span: 1 },
  '1W': { type: 'week', span: 1 },
  '1M': { type: 'month', span: 1 }
}

// --- Symbol search UI ---

let searchTimeout: ReturnType<typeof setTimeout> | null = null
const symbolInput = document.getElementById('symbol-input') as HTMLInputElement
const symbolResults = document.getElementById('symbol-results')!

symbolInput.addEventListener('input', () => {
  if (searchTimeout) clearTimeout(searchTimeout)
  const query = symbolInput.value.trim()
  if (!query) {
    symbolResults.style.display = 'none'
    return
  }
  searchTimeout = setTimeout(async () => {
    const results = await feed.searchSymbols(query)
    renderResults(results)
  }, 300)
})

symbolInput.addEventListener('focus', () => {
  if (symbolResults.children.length > 0) {
    symbolResults.style.display = 'block'
  }
})

document.addEventListener('click', (e) => {
  if (!(e.target as HTMLElement).closest('#symbol-search')) {
    symbolResults.style.display = 'none'
  }
})

function renderResults (results: any[]) {
  symbolResults.innerHTML = ''
  if (!results.length) {
    symbolResults.style.display = 'none'
    return
  }
  results.forEach((item: any) => {
    const div = document.createElement('div')
    div.className = 'symbol-item'
    div.innerHTML = `<span class="ticker">${item.ticker}</span><span class="name">${item.name || ''}</span>`
    div.addEventListener('click', () => {
      symbolInput.value = item.ticker
      symbolResults.style.display = 'none'
      const sym: SymbolInfo = {
        ticker: item.ticker,
        pricePrecision: item.pricePrecision ?? 2,
        volumePrecision: item.volumePrecision ?? 0
      }
      chart?.setSymbol(sym)
    })
    symbolResults.appendChild(div)
  })
  symbolResults.style.display = 'block'
}

// --- Period selector ---

const periodSelect = document.getElementById('period-select') as HTMLSelectElement
periodSelect.addEventListener('change', () => {
  const period = RESOLUTION_TO_PERIOD[periodSelect.value]
  if (period && chart) {
    chart.setPeriod(period)
  }
})

// --- Init ---

createChart()

// --- HMR ---

if (import.meta.hot) {
  import.meta.hot.accept(() => {
    dispose('chart')
    document.getElementById('chart')!.removeAttribute('k-line-chart-id')
    document.getElementById('chart')!.innerHTML = ''
    createChart()
  })
}
