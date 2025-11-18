import { useDataStore } from '@/stores/data'

/** centralise: click → store updates, CAPEX/EAL sums, grid index */
export function useAssetSelection () {
  const store = useDataStore()
  const {
    gridData,
    hbomDefinitions,
    setSelectedAsset,
    setSelectedGridIndex,
  } = store

  /* lookup table built once per call */
  function buildHbomMap () {
    const map = {}
    const walk = (node) => {
      map[node.uuid] = node
      node.children?.forEach(walk)
    }
    hbomDefinitions.components?.forEach(walk)
    return map
  }

  /** selects asset + grid index + capex/eal totals */
  function onAssetSelected (asset) {
    const hbomByUuid = buildHbomMap()
    const sel =
      hbomByUuid[asset.id]   ??
      hbomByUuid[asset.uuid] ?? asset

    setSelectedAsset(sel)

    // nearest grid cell
    const findIdx = (lat, lon) => {
      let best = -1
      let minD = Infinity
      gridData.forEach((g, i) => {
        const cLat = (g.bounds.min_lat + g.bounds.max_lat) / 2
        const cLon = (g.bounds.min_lon + g.bounds.max_lon) / 2
        const d = Math.hypot(cLat - lat, cLon - lon)
        if (d < minD) { minD = d; best = i }
      })
      return best
    }
    setSelectedGridIndex(findIdx(asset.latitude, asset.longitude))
  }

  return { onAssetSelected }
}