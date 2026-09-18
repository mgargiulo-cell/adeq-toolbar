---
name: reference-agent-audit-sql
description: "SQL COMPLETO y probado para auditar la labor del agente (envíos, descartes, fuentes, GEO por nivel, funnel, rebotes, Monday). El user lo pide como \"el SQL de auditoría del agente\"."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 63cc8b14-2ceb-446a-9c44-9c75690d5923
  modified: 2026-07-21T13:04:23.537Z
---

Cuando el user pide "el SQL para chequear la labor del agente" / "auditoría del agente", darle ESTE
bloque entero (una sola corrida). Hora Buenos Aires (-03). Verificado contra las columnas reales
(2026-07-21). **GOTCHA que ya rompió una vez:** en un UNION, toda rama con ORDER BY/LIMIT DEBE ir
envuelta en `SELECT * FROM ( ... ) tN` — si no, "syntax error at or near UNION". Este patrón ya viene así.

Columnas confirmadas: `toolbar_agent_actions`(action, domain, email_to, user_email, reason, details jsonb
{email,source,traffic,geo,language,bounce_type,failed_email}, created_at, template_id). `toolbar_review_queue`
(domain, traffic, geo, geos_all, language, category, emails, contact_phone, score, status, source, created_at).
`toolbar_csv_queue`(domain, status, source, uploaded_at, error_message). `toolbar_keyword_yield`(phrase,
searches, found, fresh, qualified). `toolbar_config`(key, value — NO tiene updated_at). Ventana default 5 días.

Niveles GEO (prioridad del user): 1 LATAM(Sudamérica+México) · 2 Centroamérica · 3 España ·
4 Europa/Asia/África/MENA · 5 Oceanía/USA/Canadá (cap 10%).

```sql
SELECT seccion, k, valor, detalle FROM (

  SELECT * FROM (SELECT 1 AS ord, '① SALUD' AS seccion, key AS k, value AS valor,
    CASE WHEN value ~ '^\d{4}-\d{2}-\d{2}T' THEN round(EXTRACT(epoch FROM (now()-value::timestamptz))/60)::text||' min' ELSE '' END AS detalle
    FROM toolbar_config WHERE key IN ('auto_heartbeat_at','apollo_calls_month','apollo_calls_month_period',
      'rapidapi_calls_month','autogoogle_serper_used','serper_contact_used','autogoogle_last_error',
      'autogoogle_fresh_rate','target_geo')) t1

  UNION ALL SELECT * FROM (SELECT 2, '② ACCIONES 5d', action, count(*)::text, count(DISTINCT domain)::text||' dom'
    FROM toolbar_agent_actions WHERE created_at >= now()-interval '5 days' GROUP BY action) t2

  UNION ALL SELECT * FROM (SELECT 3, '③ POR QUÉ NO ENVIÓ', coalesce(reason,'(sin motivo)'), count(*)::text,
    count(DISTINCT domain)::text||' dom · ej:'||min(domain)
    FROM toolbar_agent_actions WHERE action IN ('skipped','reengagement_exhausted') AND created_at >= now()-interval '5 days'
    GROUP BY reason ORDER BY count(*) DESC LIMIT 15) t3

  UNION ALL SELECT * FROM (SELECT 4, '④ ÚLTIMOS ENVÍOS', domain||' → '||coalesce(email_to,'?'),
    to_char(created_at AT TIME ZONE 'America/Argentina/Buenos_Aires','DD/MM HH24:MI'),
    'src:'||coalesce(details->>'source','?')||' · '||coalesce(details->>'geo','?')
    FROM toolbar_agent_actions WHERE action='sent' ORDER BY created_at DESC LIMIT 30) t4

  UNION ALL SELECT * FROM (SELECT 5, '⑤ FUENTE DEL EMAIL', coalesce(details->>'source','(?)'), count(*)::text,
    round(100.0*count(*)/sum(count(*)) OVER (),1)::text||'%'
    FROM toolbar_agent_actions WHERE action IN ('sent','re_sent','secondary_sent') AND created_at >= now()-interval '5 days'
    GROUP BY details->>'source' ORDER BY count(*) DESC) t5

  UNION ALL SELECT * FROM (SELECT 6, '⑥ NIVEL GEO ENVIADO',
    CASE WHEN upper(coalesce(details->>'geo','?')) IN ('ARGENTINA','BRAZIL','COLOMBIA','CHILE','PERU','ECUADOR','VENEZUELA','URUGUAY','PARAGUAY','BOLIVIA','MEXICO') THEN 'N1 LATAM'
         WHEN upper(coalesce(details->>'geo','?')) IN ('COSTA RICA','PANAMA','GUATEMALA','HONDURAS','NICARAGUA','EL SALVADOR','DOMINICAN REPUBLIC','CUBA','PUERTO RICO') THEN 'N2 Centroam'
         WHEN upper(coalesce(details->>'geo','?'))='SPAIN' THEN 'N3 España'
         WHEN upper(coalesce(details->>'geo','?')) IN ('UNITED STATES','CANADA','AUSTRALIA','NEW ZEALAND') THEN 'N5 Anglo'
         ELSE 'N4 Eur/Asia/Afr' END,
    count(*)::text, round(100.0*count(*)/sum(count(*)) OVER (),1)::text||'%'
    FROM toolbar_agent_actions WHERE action='sent' AND created_at >= now()-interval '5 days' GROUP BY 3 ORDER BY 3) t6

  UNION ALL SELECT * FROM (SELECT 7, '⑦ IDIOMA vs GEO',
    coalesce(details->>'language','?')||' → '||upper(coalesce(details->>'geo','?')), count(*)::text, ''
    FROM toolbar_agent_actions WHERE action='sent' AND created_at >= now()-interval '5 days'
    GROUP BY 3 ORDER BY count(*) DESC LIMIT 15) t7

  UNION ALL SELECT * FROM (SELECT 8, '⑧ TIPO EMAIL',
    CASE WHEN split_part(email_to,'@',1) ~* '^(info|contact|contacto|contato|hello|hola|mail|admin|general|webmaster|office|support|soporte)$' THEN 'genérico info@'
         WHEN split_part(email_to,'@',1) ~* '(ventas|sales|comercial|publicidad|publicidade|marketing|prensa|press|redaccion|redazione)' THEN 'ROL COMERCIAL'
         ELSE 'persona' END,
    count(*)::text, round(100.0*count(*)/sum(count(*)) OVER (),1)::text||'%'
    FROM toolbar_agent_actions WHERE action='sent' AND email_to IS NOT NULL AND created_at >= now()-interval '5 days'
    GROUP BY 3 ORDER BY count(*) DESC) t8

  UNION ALL SELECT * FROM (SELECT 9, '⑨ DOMINIO DUPLICADO', domain, count(*)::text||' envíos',
    to_char(max(created_at) AT TIME ZONE 'America/Argentina/Buenos_Aires','DD/MM')
    FROM toolbar_agent_actions WHERE action IN ('sent','re_sent','secondary_sent') AND created_at >= now()-interval '30 days'
    GROUP BY domain HAVING count(*)>1 ORDER BY count(*) DESC LIMIT 10) t9

  UNION ALL SELECT * FROM (SELECT 10, '⑩ REBOTE x TIPO', coalesce(details->>'bounce_type','?'), count(*)::text,
    count(DISTINCT domain)::text||' dom'
    FROM toolbar_agent_actions WHERE action='bounce_detected' AND created_at >= now()-interval '30 days'
    GROUP BY 3 ORDER BY count(*) DESC) t10

  UNION ALL SELECT * FROM (SELECT 11, '⑪ FUNNEL x FUENTE', source,
    round(100.0*count(*) FILTER (WHERE status='done')/NULLIF(count(*) FILTER (WHERE status IN ('done','skipped','frozen')),0),1)::text||'% pasa',
    count(*) FILTER (WHERE status='done')::text||'d/'||count(*) FILTER (WHERE status='skipped')::text||'s/'||count(*) FILTER (WHERE status='frozen')::text||'f'
    FROM toolbar_csv_queue GROUP BY source ORDER BY count(*) DESC) t11

  UNION ALL SELECT * FROM (SELECT 12, '⑫ DESCARTE DISCOVERY', regexp_replace(coalesce(error_message,'(?)'),':.*$',''), count(*)::text, ''
    FROM toolbar_csv_queue WHERE status IN ('skipped','frozen') AND uploaded_at >= now()-interval '5 days'
    GROUP BY 3 ORDER BY count(*) DESC LIMIT 12) t12

  UNION ALL SELECT * FROM (SELECT 13, '⑬ PROSPECTS 5d x FUENTE', source, count(*)::text,
    count(*) FILTER (WHERE emails IS NOT NULL AND emails<>'{}')::text||' c/mail · '||count(*) FILTER (WHERE contact_phone IS NOT NULL)::text||' c/tel'
    FROM toolbar_review_queue WHERE created_at >= now()-interval '5 days' GROUP BY source ORDER BY count(*) DESC) t13

  UNION ALL SELECT * FROM (SELECT 14, '⑭ POOL x NIVEL',
    CASE WHEN upper(coalesce(geo,'?')) IN ('ARGENTINA','BRAZIL','COLOMBIA','CHILE','PERU','ECUADOR','VENEZUELA','URUGUAY','PARAGUAY','BOLIVIA','MEXICO') THEN 'N1 LATAM'
         WHEN upper(coalesce(geo,'?')) IN ('COSTA RICA','PANAMA','GUATEMALA','HONDURAS','NICARAGUA','EL SALVADOR','DOMINICAN REPUBLIC','CUBA','PUERTO RICO') THEN 'N2 Centroam'
         WHEN upper(coalesce(geo,'?'))='SPAIN' THEN 'N3 España'
         WHEN upper(coalesce(geo,'?')) IN ('UNITED STATES','CANADA','AUSTRALIA','NEW ZEALAND') THEN 'N5 Anglo'
         ELSE 'N4 Eur/Asia/Afr' END,
    count(*)::text, round(100.0*count(*)/sum(count(*)) OVER (),1)::text||'%'
    FROM toolbar_review_queue WHERE status='pending' GROUP BY 3 ORDER BY 3) t14

  UNION ALL SELECT * FROM (SELECT 15, '⑮ POOL REALIDAD',
    CASE WHEN emails IS NULL OR emails='{}' THEN 'SIN email (no enviable)'
         WHEN upper(coalesce(geo,'?')) IN ('UNITED STATES','CANADA','AUSTRALIA','NEW ZEALAND') THEN 'Anglo (nivel 5, al final)'
         ELSE 'enviable' END,
    count(*)::text, round(100.0*count(*)/sum(count(*)) OVER (),1)::text||'%'
    FROM toolbar_review_queue WHERE status='pending' GROUP BY 3 ORDER BY count(*) DESC) t15

  UNION ALL SELECT * FROM (SELECT 16, '⑯ AUTOGOOGLE YIELD', 'filas='||count(*)::text,
    coalesce(sum(searches),0)::text||' búsq', coalesce(sum(fresh),0)::text||' frescos / '||coalesce(sum(qualified),0)::text||' calif'
    FROM toolbar_keyword_yield) t16

  UNION ALL SELECT * FROM (SELECT 17, '⑰ MONDAY', action, count(*)::text, ''
    FROM toolbar_agent_actions WHERE action IN ('monday_ok','monday_failed','monday_recovered','monday_rescue_failed') AND created_at >= now()-interval '5 days'
    GROUP BY action ORDER BY count(*) DESC) t17

) x ORDER BY ord, valor DESC;
```

Qué mirar (referencia rápida para el análisis):
- ③ si `no_email_after_enrichment` domina → cuello de botella = contacto (Server ya reforzado en qualify+envío+rescate).
- ⑥ vs ⑭: nivel GEO enviado vs pool. Querés N1/N2/N3 arriba, N5 Anglo ≤10%.
- ⑦ `en → SPAIN`/`en → MEXICO` = pitch en idioma equivocado (error caro).
- ⑧ si "genérico info@" > "ROL COMERCIAL" → el ranking no prioriza comercial como el user quiere.
- ⑨ dominio con 2+ = posible reenvío no deseado.
- ⑩ si `hard` domina → esa tanda trae emails muertos.
- ⑪ Majestic venía 19-20% (descarta 80%); ⑯ yield debe tener filas>0 y qualified subiendo.
- ⑮ "SIN email" alto = confirma urgencia del refuerzo de contacto.

Baseline pre-fix 2026-07-17 a batir: envíos GEO ~Hispano 19% / pool Hispano 9,6% / Anglo 36-77%.
Relacionado: [[adeq-toolbar-estado-y-pendientes]], [[reference-billing-cycles]].


## Las dos preguntas del user (2026-08-28) — SQL listo para contestar al instante
**"¿Qué fuente performa mejor día a día?"** (altas sobrevive purgas; % de conversión usar
`processed_at` sobre `toolbar_csv_queue`, que desde el fix del 28/08 conserva los done 30 días):
```sql
select created_at::date dia, source, count(*) altas
from toolbar_review_queue where created_at > now() - interval '7 days'
group by 1,2 order by 1 desc, 3 desc;
```
**"¿Qué tipo de email responde más?"** (respuesta REAL, sin OOO):
```sql
with t as (select email_sent_to, source, responded_at, response_type
           from toolbar_response_tracking where sent_at is not null)
select case when source='manual_extra' then 'manual (adicional)'
            when source='apollo' then 'apollo (persona)'
            when email_sent_to ~* '^(publicidad|ads|sales|ventas|comercial|marketing)@' then 'rol comercial'
            when email_sent_to ~* '^(info|contact|contacto|contato|hello|hola|admin|prensa)@' then 'genérico'
            else 'persona con nombre' end tipo,
       count(*) enviados,
       count(*) filter (where responded_at is not null and response_type<>'out_of_office') resp,
       round(100.0*count(*) filter (where responded_at is not null and response_type<>'out_of_office')/count(*),1) pct
from t group by 1 order by 4 desc;
```
Medido 2026-08-28 (histórico completo): apollo 9,3% · manual adicional 7,7% · genérico 6,5% ·
persona scrapeada 6,3% · rol comercial 6,3%. **El boletín diario ya trae este bloque** (TIPOS
DE EMAIL QUE RESPONDEN, ventana 30d, mínimo 10 envíos por tipo).
⚠️ La purga borró 429 done el 27/08 (cortaba por uploaded_at — ya corregido a processed_at):
la conversión por fuente de ANTES del 28/08 está subcontada en csv_queue; usar las altas.
