import { CircleMarker, MapContainer, Popup, TileLayer, Tooltip } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import ChartCard from "./ChartCard";
import { MONO } from "../../lib/echartsTheme";
import { formatInt } from "../../lib/format";

const ZONA_LESTE_CENTER = [-23.555, -46.48];
const MIN_RADIUS = 8;
const MAX_RADIUS_ADD = 24;

const FAINT_RGB = [212, 212, 212]; // MONO.faint
const INK_RGB = [10, 10, 10]; // MONO.ink

function intensityColor(t) {
  const [r, g, b] = FAINT_RGB.map((c, i) => Math.round(c + (INK_RGB[i] - c) * t));
  return `rgb(${r}, ${g}, ${b})`;
}

/** Mapa de bolhas (Leaflet + OpenStreetMap) dos bairros da Zona Leste, por volume de ocorrências. */
export default function ZonaLesteMap({ data }) {
  const isEmpty = !data || data.length === 0;
  const maxTotal = isEmpty ? 0 : Math.max(...data.map((d) => d.total));

  return (
    <ChartCard
      title="Bairros da Zona Leste"
      isEmpty={isEmpty}
      emptyMessage="Sem bairros geocodificados da Zona Leste para os filtros selecionados."
    >
      <div className="rounded-lg overflow-hidden border border-border" style={{ height: 460 }}>
        <MapContainer
          center={ZONA_LESTE_CENTER}
          zoom={12}
          style={{ height: "100%", width: "100%" }}
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          {(data ?? []).map((d) => {
            const t = maxTotal ? Math.sqrt(d.total / maxTotal) : 0;
            return (
              <CircleMarker
                key={d.key}
                center={[d.lat, d.lon]}
                radius={MIN_RADIUS + t * MAX_RADIUS_ADD}
                pathOptions={{
                  color: MONO.ink,
                  weight: 1,
                  fillColor: intensityColor(t),
                  fillOpacity: 0.75,
                }}
              >
                <Tooltip direction="top" offset={[0, -4]}>
                  {d.label}
                </Tooltip>
                <Popup>
                  <strong>{d.label}</strong>
                  <br />
                  Total: {formatInt(d.total)}
                  <br />
                  Furtos: {formatInt(d.furtos)}
                  <br />
                  Roubos: {formatInt(d.roubos)}
                </Popup>
              </CircleMarker>
            );
          })}
        </MapContainer>
      </div>
      <p className="text-xs text-muted px-2 pt-2">
        Círculos posicionados no centróide geocodificado de cada bairro; área proporcional ao volume
        de ocorrências no recorte filtrado. Fonte: SSP-SP.
      </p>
    </ChartCard>
  );
}
