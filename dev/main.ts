import { init, dispose } from '../src/index'

function generateKLineData (count = 500): Array<{
  timestamp: number
  open: number
  high: number
  low: number
  close: number
  volume: number
}> {
  const data = []
  const now = Date.now()
  const oneMinute = 60 * 1000
  let basePrice = 5000
  let baseVolume = 200

  for (let i = 0; i < count; i++) {
    const change = (Math.random() - 0.48) * basePrice * 0.04
    const open = basePrice + change
    const close = open + (Math.random() - 0.5) * basePrice * 0.03
    const high = Math.max(open, close) + Math.random() * basePrice * 0.01
    const low = Math.min(open, close) - Math.random() * basePrice * 0.01
    const volume = baseVolume + Math.random() * 50

    data.push({
      timestamp: now - (count - i) * oneMinute,
      open: +open.toFixed(2),
      high: +high.toFixed(2),
      low: +low.toFixed(2),
      close: +close.toFixed(2),
      volume: +volume.toFixed(0)
    })

    basePrice = close
    baseVolume = volume
  }

  return data
}

function createChart (): void {
  const chart = init('chart')
  if (chart === null) return

  chart.applyNewData(generateKLineData())
  chart.createIndicator('MA', false, { id: 'candle_pane' })
  chart.createIndicator('VOL')
}

createChart()

if (import.meta.hot) {
  import.meta.hot.accept(() => {
    dispose('chart')
    document.getElementById('chart')!.removeAttribute('k-line-chart-id')
    document.getElementById('chart')!.innerHTML = ''
    createChart()
  })
}
