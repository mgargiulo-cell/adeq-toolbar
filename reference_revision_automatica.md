---
name: reference-revision-automatica
description: "La rutina en la nube que revisa la toolbar martes y viernes: qué ve (salud_snapshot), qué puede tocar, dónde está su manual, cómo se rota la clave y cómo se depura una corrida"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-18T11:18:01.318Z
---

Pedido del user (2026-09-18): *"¿no podemos hacer que vos leas 1 o 2 veces a la semana este análisis
y hagas las mejoras por tu cuenta? Porque si no cada semana tengo yo que estar escribiendo los
cambios."* Eligió el camino A de tres ("Soluciona todos los problemas, adelante").

## Qué es
Rutina de Claude Code en la nube **`trig_0166ZUh8B1xWo6SrsVnhzRYq`** — "Revisión de salud — ADEQ
Toolbar (mar y vie)" — cron `0 11 * * 2,5` UTC (= 08:00 Argentina), modelo `claude-opus-5`, entorno
`env_01AMeLWYdMVk2muPrrjhhXv3`. Panel: https://claude.ai/code/routines/trig_0166ZUh8B1xWo6SrsVnhzRYq
Se maneja con la tool `RemoteTrigger` (`list_runs` + `get_run_log` para depurar). No se pueden
borrar por API: sólo desde el panel. Para pausarla: `update` con `enabled:false`.

## La foto viaja por GitHub (resuelto el 18/09 a la tarde, sin que el user toque nada)
La primera corrida de prueba falló: el proxy de salida del entorno rechaza `supabase.co`
(`connect_rejected — organization policy`); GitHub sí anda. Abrir la red es un ajuste manual en
claude.ai que el user no quiso hacer (*"¿por qué no dejás todo listo para que funcione?"*). El otro
entorno de su cuenta (`env_01QFAG…`) ya no existe. Solución: **`.github/workflows/foto-salud.yml`**
(martes y viernes 10:30 UTC + a mano + al tocar ese archivo) llama a `salud_snapshot` y deja
`foto-salud.json` en la **rama `salud`**, en un solo commit que se pisa (force-push). La rutina la lee
con `git fetch origin salud && git show origin/salud:foto-salud.json` y, si `generado_at` tiene más
de 24 h, frena diciendo que el workflow no corrió. Railway no despliega esa rama (tampoco `memoria`).
**La clave ahora vive DENTRO del workflow** (repo privado, que ya guarda la anon key y el secreto del
CRM en config.js) y ya NO está en el prompt de la rutina. ⚠️ Si alguna vez se le da acceso al repo a
otra persona: rotarla ese día (nuevo valor en el workflow + nuevo hash en
`toolbar_config.salud_snapshot_sha256`). Un workflow programado se apaga solo tras 60 días sin
actividad en el repo: si la foto envejece, mirar eso primero.

## Qué ve y qué no
Tiene **una copia del repo y nada más**: ni la base (el CLI de Supabase linkeado es de esta Mac), ni
Railway, ni `~/.adeq-cws.json`. Para la base se le dio UNA cosa: el RPC **`salud_snapshot(k)`**
(`sql/2026-09-18_salud_snapshot.sql`): SECURITY DEFINER, sólo lectura, agregados (health, feeder_runs,
cola por estado/motivo/error, agente_7d, altas, metricas_diarias, gasto Claude, MV) y la config por
**lista blanca** de claves. Sin API keys ni emails de leads (verificado: sólo aparecen los 3 buzones
internos). Se llama como `anon` con la anon key de `config.js`.
**La clave**: 32 bytes hex en `~/.adeq-salud-snapshot.key` (chmod 600) y en el prompt de la rutina;
en la base sólo vive su SHA-256 (`toolbar_config.salud_snapshot_sha256`). **Rotarla** = generar otra,
actualizar el hash en la base y el prompt de la rutina (`RemoteTrigger update`). Si el user pregunta
por seguridad: la clave no da escritura, no da datos de leads, y se revoca cambiando un valor.

## El manual: `REVISION-SEMANAL.md` (raíz del repo, NO en docs/ porque docs/ va al zip)
La rutina arranca sin contexto y lo lee entero. Tiene: qué puede/no puede, cómo bajar la foto, el
método de revisión, la lista de **cosas que parecen fallas y no lo son**, las reglas del user y la
entrega: rama `revision/AAAA-MM-DD` + PR contra main con 4 partes (qué estaba mal con el número ·
qué cambié · qué NO toqué y por qué · qué hace falta desde la Mac). **Nunca empuja a main.**
**Si algo cambia en las reglas del user o aparece un nuevo "parece falla y no es", hay que
actualizar ese archivo** — es la única memoria que tiene la rutina.

## Lo que NO puede y sigue siendo trabajo de esta Mac
Aplicar SQL, publicar el zip (lo marca como "requiere zip"), verificar en producción después del
deploy, y cualquier cosa del CRM. El user aprueba el PR con un toque; Railway despliega solo al mergear.
Posible paso futuro (no pedido): aprobación automática si los tests pasan.

Relacionado: [[reference_digest_diario]], [[feedback_alertas_automaticas]], [[project_security]]
