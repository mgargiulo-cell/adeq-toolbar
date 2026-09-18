---
name: project-verificar-dashboard
description: Checklist para verificar (a partir del 2026-09-04) los 5 arreglos que la sesión del dashboard tomó de PROMPT-DASHBOARD.md
metadata: 
  node_type: memory
  type: project
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-03T00:43:55.569Z
---

El 2026-09-03 se le pasó a la sesión de `adeq-dashboard` el archivo `PROMPT-DASHBOARD.md`
(está en la raíz de `adeq-toolbar`). El user avisó que **la están trabajando** y que
**hay que chequear al día siguiente si quedó bien**. Verificar, no preguntar.

⚠️ **Verificar contra la BASE y contra el código en HEAD, no contra lo que reporten.** Casi todo
lo grave de esta migración fue "arreglado" y seguía roto: ver [[feedback_lecciones_migracion]].

## Los 5 puntos y cómo se comprueba cada uno

**A) `crm_board_template_sends` estaba en 0 filas** (con 3.282 corridas en 7 días).
```sql
-- en adeq-dashboard, supabase db query --linked
select count(*) from crm_board_template_sends;                        -- tiene que ser > 0
select count(*) from crm_board_automation_runs where created_at > now() - interval '24 hours';
```
Si sigue en 0 el bug no está resuelto, aunque el código haya cambiado. Sospecha original: un
error de `supabase-js` tragado (no tira excepción, vuelve como `{error}`).

**B) `mark-live-monday` agendado pero sin `export GET`** → Vercel pega con GET y da 405.
```bash
grep -nE "^export (async )?function GET" app/api/cron/mark-live-monday/route.ts
grep -c "mark-live-monday" vercel.json
```
Válido cualquiera de las dos salidas: que exporte GET, **o** que ya no esté en `vercel.json`.
Lo que no puede quedar es agendado y muerto.

**C) El fail-open del cron de rebotes** (`app/api/cron/crm-board-bounces/route.ts`, ~línea 77).
Era `if (cfg) { ...si dice false, return... }`: con `cfg` null —fila borrada **o consulta que
falla**— seguía de largo y **corría**. Tiene que quedar **apagado por defecto** y no correr si
no puede leer la config. Confirmar que `crm_board_bounces_enabled` sigue en `false`.

**D) Vigilante de los 19 crons.** Buscar una tabla tipo `crm_cron_health` con filas recientes.
La parte que importa: que cada cron **late también cuando no encuentra trabajo** — si sólo
pinguea al hacer algo, un job sano se ve igual que uno roto (ese bug estaba en el worker y se
arregló el 03/09). Y que el umbral salga de la cadencia declarada de cada job, no de un número
fijo: con crons de 3 minutos y de 15 días, un umbral fijo no sirve para ninguno de los dos.

**E) La frontera de los rebotes escrita** donde se vea (README o cabecera del cron), no sólo
dentro del string de un `return`.

## Contexto que hay que sostener al revisar
- **Los rebotes los lee el worker, no el CRM** (service account con domain-wide delegation ve
  todas las casillas; el CRM depende de `crm_gmail_tokens`). El CRM aplica la consecuencia.
- **El mail inicial nunca sale del CRM.** Sale de la extensión o del agente.
- Estado al cierre del 03/09: 10.003 filas · 92 en negociación · 104 respuestas · **247 con
  rebote marcado** (backfill mío, de 3).

Relacionado: [[reference_crm_board_modelo]], [[feedback_lecciones_migracion]],
[[feedback_alertas_automaticas]]
