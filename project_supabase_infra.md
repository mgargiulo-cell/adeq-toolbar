---
name: project_supabase_infra
description: "Supabase infra del proyecto — instancia Micro IO-constrained, causa de caídas recurrentes, spend cap, y blindaje anti-costo"
metadata: 
  node_type: memory
  type: project
  originSessionId: 63cc8b14-2ceb-446a-9c44-9c75690d5923
---

Supabase project ref: `ticjpwimhtfkbccchfyp` (Toolbar-Adeq, Pro Plan $25/mes).

**Instancia = Micro (1GB RAM, CPU compartida, Disk IO baseline 43 Mbps + 30min/día burst).** Es el cuello de botella: CPU ~16% y RAM ~44% (sobra), pero **Disk IO llega al 64% y agota el burst budget** → Postgres deja de responder → HTTP 522/Unhealthy. Ese es el patrón de "se cae cada X días". NO es CPU ni RAM — es IO.

Caída de 2026-07-07: combinó el incidente de capacidad de Supabase (restart/resize fallando en Postgres <17.6.1.121) + el IO agotado. Se recuperó con **Restart del proyecto** (Settings→General). Si un restart falla en medio de un incidente, upgrade de Postgres primero (Infrastructure) o ticket a soporte (Pro lo tiene).

**Spend cap:** el usuario lo sacó (miedo a "unresponsive al pasar cuota"), pero el uso real está al 1-2% de la cuota (egress 3GB/250GB) → el spend cap NO era la causa de las caídas. Recomendación firme: **reactivarlo** — es la red de seguridad real contra gasto runaway y es transparente a ese nivel de uso.

**Blindaje anti-costo en el worker** (por si el cap queda off): caps mensuales/diarios en RapidAPI, Apollo (2400/mes hard cap centralizado), LLM (caps diarios + slots 1×/día), + un **fusible global** en `rapidFetchWithRetry` que corta a 250 hits/min. Único agujero: `agent_test_mode=true` saltea todos los caps → verificar que esté en false.

Ver [[project_pending]] y [[feedback_cost_awareness]].
