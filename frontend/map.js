/**
 * Texas Broadband Truth Map — map.js
 *
 * Layers:
 *   "gap"     — overstatement ratio choropleth (default)
 *   "claimed" — FCC max advertised download speed
 *   "actual"  — Ookla average download speed
 */

"use strict";

// ── Configuration ────────────────────────────────────────────────────────────

const CONFIG = {
  center: [31.2, -99.3],
  zoom: 6,
  minZoom: 5,
  maxZoom: 12,
  // Rough bounding box around Texas
  maxBounds: [[25.0, -107.5], [37.5, -92.5]],
  dataUrl: "data/processed/counties.geojson",
  tileUrl: "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
  tileAttrib: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
};

// ── Color Scales ─────────────────────────────────────────────────────────────

/**
 * Overstatement ratio scale:
 *   ≤ 1.0  accurate (green)
 *   1–2    minor (yellow-green)
 *   2–3    moderate (orange)
 *   3–4    severe (red-orange)
 *   > 4    extreme (dark red)
 */
function getRatioColor(ratio) {
  if (ratio <= 1.1) return "#2ecc71";
  if (ratio <= 1.5) return "#82e03a";
  if (ratio <= 2.0) return "#f1c40f";
  if (ratio <= 2.5) return "#e67e22";
  if (ratio <= 3.0) return "#e74c3c";
  if (ratio <= 4.0) return "#c0392b";
  return "#7b241c";
}

/**
 * Speed scale (Mbps): 0 → dark, 100+ → bright blue
 */
function getSpeedColor(mbps) {
  if (mbps < 5)  return "#1a1f3a";
  if (mbps < 10) return "#1a3460";
  if (mbps < 25) return "#1a5fa8";
  if (mbps < 50) return "#2980b9";
  if (mbps < 75) return "#3498db";
  return "#74b9ff";
}

function getLayerColor(feature, layer) {
  const p = feature.properties;
  if (layer === "gap")     return getRatioColor(p.overstatement_ratio);
  if (layer === "claimed") return getSpeedColor(p.fcc_claimed_down_mbps);
  if (layer === "actual")  return getSpeedColor(p.ookla_actual_down_mbps);
  return "#555";
}

// ── Popup builder ─────────────────────────────────────────────────────────────

function ratioClass(r) {
  if (r <= 1.5) return "accurate";
  if (r <= 2.5) return "moderate";
  if (r <= 4.0) return "severe";
  return "extreme";
}

function buildPopup(props) {
  const ratio = props.overstatement_ratio;
  const rc    = ratioClass(ratio);
  const providers = (props.top_providers || [])
    .map(p => `<span class="provider-tag">${escHtml(p)}</span>`)
    .join("");

  return `
<div class="popup-inner">
  <div class="popup-county">
    ${escHtml(props.NAME)} County
    <span class="popup-ratio ${rc}">${ratio.toFixed(2)}× overstated</span>
  </div>
  <div class="popup-grid">
    <div class="popup-stat">
      <div class="popup-stat-label">FCC Claimed ↓</div>
      <div class="popup-stat-value claimed">${props.fcc_claimed_down_mbps.toFixed(0)}<small> Mbps</small></div>
    </div>
    <div class="popup-stat">
      <div class="popup-stat-label">Ookla Actual ↓</div>
      <div class="popup-stat-value actual">${props.ookla_actual_down_mbps.toFixed(1)}<small> Mbps</small></div>
    </div>
    <div class="popup-stat">
      <div class="popup-stat-label">FCC Claimed ↑</div>
      <div class="popup-stat-value claimed">${props.fcc_claimed_up_mbps.toFixed(0)}<small> Mbps</small></div>
    </div>
    <div class="popup-stat">
      <div class="popup-stat-label">Ookla Actual ↑</div>
      <div class="popup-stat-value actual">${props.ookla_actual_up_mbps.toFixed(1)}<small> Mbps</small></div>
    </div>
  </div>
  <div class="popup-providers">
    <div class="popup-providers-label">Top fixed wireless providers</div>
    ${providers || '<span style="color:#8890a8;font-size:0.7rem">No data</span>'}
  </div>
</div>`;
}

function escHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// ── Statistics ────────────────────────────────────────────────────────────────

function computeStats(features) {
  let totalRatio = 0, maxRatio = 0, countiesAbove2 = 0;
  for (const f of features) {
    const r = f.properties.overstatement_ratio;
    totalRatio += r;
    if (r > maxRatio) maxRatio = r;
    if (r >= 2) countiesAbove2++;
  }
  return {
    avg: totalRatio / features.length,
    max: maxRatio,
    above2: countiesAbove2,
    total: features.length,
  };
}

function updateStatsBar(stats) {
  document.getElementById("stat-avg-ratio").textContent  = stats.avg.toFixed(2) + "×";
  document.getElementById("stat-max-ratio").textContent  = stats.max.toFixed(2) + "×";
  document.getElementById("stat-above2").textContent     = stats.above2;
}

// ── Legend builder ────────────────────────────────────────────────────────────

const LEGENDS = {
  gap: {
    title: "Overstatement Ratio",
    rows: [
      { color: "#2ecc71", label: "≤ 1.1× (accurate)" },
      { color: "#82e03a", label: "1.1–1.5×" },
      { color: "#f1c40f", label: "1.5–2.0×" },
      { color: "#e67e22", label: "2.0–2.5×" },
      { color: "#e74c3c", label: "2.5–3.0×" },
      { color: "#c0392b", label: "3.0–4.0×" },
      { color: "#7b241c", label: "> 4.0× (extreme)" },
    ],
  },
  claimed: {
    title: "FCC Claimed (Mbps ↓)",
    rows: [
      { color: "#1a1f3a", label: "< 5 Mbps" },
      { color: "#1a3460", label: "5–10 Mbps" },
      { color: "#1a5fa8", label: "10–25 Mbps" },
      { color: "#2980b9", label: "25–50 Mbps" },
      { color: "#3498db", label: "50–75 Mbps" },
      { color: "#74b9ff", label: "≥ 75 Mbps" },
    ],
  },
  actual: {
    title: "Ookla Actual (Mbps ↓)",
    rows: [
      { color: "#1a1f3a", label: "< 5 Mbps" },
      { color: "#1a3460", label: "5–10 Mbps" },
      { color: "#1a5fa8", label: "10–25 Mbps" },
      { color: "#2980b9", label: "25–50 Mbps" },
      { color: "#3498db", label: "50–75 Mbps" },
      { color: "#74b9ff", label: "≥ 75 Mbps" },
    ],
  },
};

function renderLegend(layer) {
  const def  = LEGENDS[layer];
  const rows = def.rows
    .map(r => `<div class="legend-row">
      <span class="legend-swatch" style="background:${r.color}"></span>
      <span>${r.label}</span>
    </div>`)
    .join("");
  document.getElementById("legend").innerHTML = `
    <h3>${def.title}</h3>
    <div class="legend-scale">${rows}</div>`;
}

// ── Main ──────────────────────────────────────────────────────────────────────

(function init() {
  // Map
  const map = L.map("map", {
    center: CONFIG.center,
    zoom:   CONFIG.zoom,
    minZoom: CONFIG.minZoom,
    maxZoom: CONFIG.maxZoom,
    maxBounds: CONFIG.maxBounds,
    maxBoundsViscosity: 0.85,
  });

  L.tileLayer(CONFIG.tileUrl, {
    attribution: CONFIG.tileAttrib,
    subdomains: "abcd",
  }).addTo(map);

  // State
  let activeLayer = "gap";
  let geojsonLayer = null;
  let geojsonData  = null;

  // Style function
  function styleFeature(feature) {
    return {
      fillColor:   getLayerColor(feature, activeLayer),
      fillOpacity: 0.75,
      color:       "#0f1117",
      weight:      0.5,
      opacity:     1,
    };
  }

  // Interaction
  function onEachFeature(feature, layer) {
    layer.on({
      mouseover(e) {
        const l = e.target;
        l.setStyle({ weight: 2, color: "#ffffff", fillOpacity: 0.9 });
        l.bringToFront();
      },
      mouseout(e) {
        geojsonLayer.resetStyle(e.target);
      },
      click(e) {
        const props = feature.properties;
        layer.bindPopup(buildPopup(props), { maxWidth: 300 }).openPopup();
        map.fitBounds(e.target.getBounds(), { padding: [60, 60] });
      },
    });
  }

  // Load GeoJSON
  fetch(CONFIG.dataUrl)
    .then(r => {
      if (!r.ok) throw new Error(`HTTP ${r.status} loading counties.geojson`);
      return r.json();
    })
    .then(data => {
      geojsonData = data;

      geojsonLayer = L.geoJSON(data, {
        style:         styleFeature,
        onEachFeature: onEachFeature,
      }).addTo(map);

      const stats = computeStats(data.features);
      updateStatsBar(stats);
      renderLegend(activeLayer);
    })
    .catch(err => {
      console.error("Failed to load GeoJSON:", err);
      document.getElementById("map").insertAdjacentHTML("afterbegin",
        `<div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);
                     background:#1a1d27;border:1px solid #e74c3c;border-radius:8px;
                     padding:16px 24px;z-index:9999;color:#e74c3c;text-align:center">
           <strong>Could not load map data</strong><br>
           <small style="color:#8890a8">${err.message}</small>
         </div>`
      );
    });

  // Layer toggle
  document.querySelectorAll(".layer-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const layer = btn.dataset.layer;
      if (layer === activeLayer) return;
      activeLayer = layer;

      document.querySelectorAll(".layer-btn").forEach(b =>
        b.classList.toggle("active", b.dataset.layer === layer)
      );

      if (geojsonLayer) {
        geojsonLayer.setStyle(styleFeature);
      }
      renderLegend(layer);
    });
  });

})();
