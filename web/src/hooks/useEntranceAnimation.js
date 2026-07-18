import { useEffect, useRef } from "react";
import anime from "animejs";

/**
 * Anima a entrada do dashboard quando os dados ficam prontos:
 * header fade/slide-in, depois KPI cards em stagger, depois os gráficos.
 * Roda uma única vez por transição para "ready" (evita re-animar a cada filtro).
 */
export function useEntranceAnimation(ready) {
  const played = useRef(false);

  useEffect(() => {
    if (!ready || played.current) return;
    played.current = true;

    const timeline = anime.timeline({ easing: "easeOutQuad" });

    timeline
      .add({
        targets: "#app-header",
        opacity: [0, 1],
        translateY: [-8, 0],
        duration: 420,
      })
      .add(
        {
          targets: '[data-animate="map"]',
          opacity: [0, 1],
          translateY: [16, 0],
          duration: 460,
        },
        "-=200"
      )
      .add(
        {
          targets: '[data-animate="metric-card"]',
          opacity: [0, 1],
          translateY: [14, 0],
          delay: anime.stagger(60),
          duration: 420,
        },
        "-=200"
      )
      .add(
        {
          targets: '[data-animate="chart-card"]',
          opacity: [0, 1],
          translateY: [18, 0],
          delay: anime.stagger(90),
          duration: 480,
        },
        "-=150"
      );
  }, [ready]);
}
