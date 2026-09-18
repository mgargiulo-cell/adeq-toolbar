---
name: Cost awareness — no speculative API calls
description: Never call paid APIs (Gemini, Apollo) speculatively in automated flows. Only on-demand when user decides.
type: feedback
originSessionId: c09e269f-e226-42ef-8293-d9448f31fa9c
---
Never call paid APIs (Gemini, Apollo, etc.) in automated/batch flows unless the user has explicitly decided to act on that item.

**Why:** Auto-prospector was calling Gemini twice per domain to detect contacts and generate pitches. With hundreds of domains processed daily, this cost $10 in 2 days. Most pitched domains got rejected anyway.

**How to apply:** Any automated pipeline (auto-prospector, batch processing) should only gather free/cheap data (traffic, page scrape, similar sites). Expensive operations (pitch generation, AI contact detection) happen only when the user manually clicks a button to validate/prospect that specific domain.

**MATIZ APOLLO (2026-06-30):** El user autorizó explícitamente PAGAR Apollo como FALLBACK cuando el scrape no encuentra email ("si no se encuentra en el scrapper, podemos ir a apollo y pagar por el dato"). El worker ya hace: scrape gratis PRIMERO → Apollo solo si scrape vacío → unlock pago solo si traffic ≥399K (guarda de costo). Eso está OK y alineado. Gemini sigue prohibido en auto. La prioridad es que el email NO falle: si un MB lo encuentra a mano, el worker tiene que encontrarlo (por eso se igualó el extractor del worker al del dashboard).


## ⚠️ ACTUALIZACIÓN 2026-08-24 — Apollo SÍ se usa en automático

El user cambió el criterio: *"es un plan que me sale caro y debemos aprovecharlo sí o sí"*.
La regla original —no llamar APIs pagas en flujos automáticos— nació cuando Apollo se
gastaba sin control. Hoy el problema es el opuesto: **137 llamadas de 2.500 en 12 días, el
5,5% de algo que se paga igual**, y los créditos NO se acumulan.

**Qué cambió:** Apollo ya no es solo el último recurso cuando el rastreo gratis no encuentra
NADA. También se llama cuando el lead tiene **solo direcciones genéricas** (`info@`,
`contacto@`). Medido en el pool: 351 leads sin ningún email + 127 con solo genéricos = 478
donde Apollo aporta. Es la única vía de las trece que devuelve una PERSONA con nombre y
cargo, que es lo que hace que un mail se conteste.

**Los topes siguen firmes:** 2.250/mes (10% de colchón sobre 2.500) y 250/día. Y hay un
detector nuevo, `vigilarAprovechamientoDeApollo`, que avisa si la proyección del ciclo queda
por debajo del 50% del plan. Es el primer detector que vigila que NO desperdiciemos, no que
no nos pasemos.

Gemini sigue con la regla original: solo on-demand.
