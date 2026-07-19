import { useEffect, useRef } from "react";
import anime from "animejs";

/**
 * Anima a entrada do dashboard quando os dados ficam prontos:
 * sidebar/topbar em fade, KPIs em stagger, depois o mapa.
 * Roda uma única vez por transição para "ready" (evita re-animar a cada
 * filtro) e respeita `prefers-reduced-motion`.
 */
export function useEntranceAnimation(ready) {
  const played = useRef(false);

  useEffect(() => {
    if (!ready || played.current) return;
    played.current = true;

    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;

    const timeline = anime.timeline({ easing: "easeOutQuad" });

    timeline
      .add({
        // opacidade apenas: o transform do drawer mobile é controlado por CSS
        targets: '[data-animate="sidebar"]',
        opacity: [0, 1],
        duration: 380,
      })
      .add(
        {
          targets: '[data-animate="topbar"]',
          opacity: [0, 1],
          translateY: [-6, 0],
          duration: 360,
        },
        "-=220"
      )
      .add(
        {
          targets: '[data-animate="kpi"]',
          opacity: [0, 1],
          translateY: [10, 0],
          delay: anime.stagger(55),
          duration: 380,
        },
        "-=200"
      )
      .add(
        {
          targets: '[data-animate="map"], [data-animate="chart-card"]',
          opacity: [0, 1],
          translateY: [14, 0],
          duration: 420,
        },
        "-=180"
      );
  }, [ready]);
}
