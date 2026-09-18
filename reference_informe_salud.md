---
name: reference-informe-salud
description: "El mail del boletín por sección (enviarResumenSalud): qué trae, qué se movió a los lunes y por qué. NO confundir con parteDelDia"
metadata: 
  node_type: memory
  type: reference
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-08-31T16:19:25.111Z
---

⚠️ **Son DOS mails diarios distintos y es fácil confundirlos:**
- **`parteDelDia`** (línea ~8759, sale 21h Madrid) → las 5 métricas de [[reference_4_metricas]]
  y el trabajo a mano de cada MB. Ver [[reference_parte_diario]].
- **`enviarResumenSalud`** (línea ~23450, cada 24h) → **el boletín por sección + las alertas.**
  Es el que el user suele pegar cuando dice *"mails con errores recibidos para revisar"*.

## Cómo está armado el resumen de salud
1. **El titular** (agregado 31/08) — veredicto en una línea + los envíos de hoy contra el
   objetivo. Antes abría con "Resumen de los últimos N días" y caía directo en nueve secciones:
   para saber si había que hacer algo había que leerlas todas, y son las mismas nueve todos los
   días. El titular no agrega ningún dato, ordena los que ya estaban.
2. **`📋 CÓMO RINDIÓ CADA SECCIÓN (24h)`** — de `_boletinPorSeccion`.
3. `✅ SE ARREGLÓ SOLO` · `🔴 NUEVO` · `🟡 NUEVO` · `⏳ SIGUE IGUAL` (crónico ≥3 días).
4. **Errores concretos** — motivos reales agrupados con un ejemplo de cada uno, para que el
   user pueda copiar y pegar sin ir a la base.

## Las secciones del boletín
Diarias: **ENVÍO (hoy)** · DESCUBRIMIENTO · BÚSQUEDA DE EMAILS · COLA · MONDAY→PROSPECTS ·
RE-TRABAJO · **REBOTES (7d)**.

Solo los **lunes** (`_esDiaDeLento`, usa `_spainWeekday()`): TIPOS DE EMAIL QUE RESPONDEN (30d),
PLANTILLAS POR IDIOMA, APIS. Miran ventanas de 30 días: entre ayer y hoy se mueven decimales,
pero ocupaban lugar todos los días y empujaban hacia abajo lo que sí cambió.
**El razonamiento, que vale para cualquier informe recurrente:** un mail que se lee a diario
compite contra sí mismo. Cada renglón que siempre dice lo mismo entrena a saltear, y el día que
ese renglón cambie tampoco se va a leer. Las alertas siguen siendo diarias.

**REBOTES va POR BUZÓN a propósito** (agregado 31/08): el promedio de los tres escondía el caso
que importa. Ese día sales@ estaba en 6,5% y dhorovitz@ en 1,0% con el MISMO mix de fuentes, y
el mail no lo decía en ningún lado.

## Dos trampas ya pisadas acá
**La ventana tiene que ser la misma que la regla que se juzga.** El bloque ENVÍO contaba 24h
corridas mientras la alerta de cupo contaba el día calendario: el MISMO mail decía "72 de 60,
24 cada uno" arriba y "16 de 20" abajo. Los dos números correctos, contradiciéndose — que es la
peor forma de informar, porque obliga a desconfiar de todo el resto. Ahora ENVÍO arranca en la
medianoche de Madrid. Las métricas de CAUDAL (altas, cola, re-trabajo) se quedan en 24h
corridas, que ahí sí es la lectura correcta.

**Los objetivos se derivan, nunca se clavan.** `_obj = cupo × nº de buzones` leyendo
`agent_enabled_users`. Cuando el agente pasó de 3 a 2 buzones, el mail siguió diciendo "de 60"
hasta que se derivó.

Relacionado: [[reference_alertas_falsos_positivos]], [[feedback_alertas_automaticas]],
[[reference_parte_diario]], [[reference_4_metricas]]
