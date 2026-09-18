---
name: feedback-lecciones-migracion
description: "Errores concretos que cometí migrando el CRM el 2026-09-02 y las reglas que salieron de ahí: supabase-js falla abierto, verificar los replace, no importar lo que no está en main"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-02T18:52:59.606Z
---

Salieron de una auditoría con tres agentes sobre mi propio trabajo del día. **Casi todo lo
grave lo había introducido yo esa misma jornada.** Valen más que el código que arreglaron.

## ⚠️⚠️ `supabase-js` NO TIRA EXCEPCIÓN — un `try/catch` sobre una query es CÓDIGO MUERTO
`shouldThrowOnError` es false por defecto; hasta un fallo de red vuelve como `{ error }`.
Verificado con una clave inválida: devolvió `{error}` sin lanzar.

Mis endpoints `dominios-activos` y `reciclables` tenían un catch que prometía *"ante la duda no
devuelvo nada"* y **hacían lo contrario**: si la vista de clientes fallaba, el Set quedaba
vacío y respondían **200 con la lista sin filtrar** — 129 clientes que facturan dados por
libres. Y la toolbar no podía notarlo: 2.760 dominios pasan holgados su piso de sanidad de 100.

**Siempre desestructurar `error` y cortar con 503.**
**Why:** fallar abierto en la lista que decide a quién escribirle es el error más caro del
sistema, y encima invisible.
**How to apply:** en cada `await supabase...` sacar `{ data, error }` y chequear `error`. Si el
comentario dice "ante la duda no", el código tiene que poder cumplirlo.

## ⚠️ Verificar SIEMPRE que un `replace` haya matcheado
Mi `s.replace()` sobre `PREFIJOS_BLOQUEANTES` no encontró el texto (otra sesión lo había
editado), no avisó, y **reporté el arreglo como hecho**. El commit quedó con un mensaje que
describe un cambio que no está en el diff.
**Why:** es exactamente el fallo silencioso que le vengo señalando al código, cometido por mí.
**How to apply:** `assert old in s` en todo reemplazo, sin excepción. Y releer el diff antes de
dar algo por hecho.

## 💥 Un import se resuelve contra el REPO, no contra mi carpeta
Migré mis endpoints a un helper que vi en el disco. Estaba **sin commitear**, sólo en el
working tree de otra sesión: Vercel falló con "Module not found" y **main quedó sin compilar**,
así que tampoco llegó el cambio que vino después.
**Why:** perdí ~20 minutos midiendo un endpoint que nunca se había redeployado, culpando al
caché.
**How to apply:** antes de importar algo ajeno, `git status` / `git grep` en HEAD. Y si un
deploy no se refleja, **sospechar del build antes que del caché**.

## Arreglar una punta sin probar la otra no arregla nada
El worker mandaba `source: 'agente'` y `sync-toolbar` lo **tiraba en la puerta** (hardcodeado
en `'toolbar'`). Di el bug por resuelto sin probar el recorrido completo: el parte diario
siguió contando los ~60 envíos diarios del agente como trabajo a mano.
**How to apply:** probar de punta a punta, con un dato que atraviese los dos sistemas.

## Una tabla paralela a un desplegable SIEMPRE termina divergiendo
Copié el mapa de estados de `MONDAY_STATES`, que estaba desactualizado: decía índice 4 =
"Rebotado" cuando el board real decía "Propuesta Vigente (T)". Y el 4 era el DEFECTO del
formulario, así que **todo push manual entraba como un rebote** — y "Rebotado" no bloquea, con
lo cual el agente le volvía a escribir.
**How to apply:** leer la etiqueta **del propio `<select>`**. Lo que el MB ve es lo que se
manda, y es imposible que diverjan.

## Migrar rompe contratos silenciosos
Al cambiar `fetchManualSendsFromMonday` de `{ok, items}` a un array pelado, `monday.ok` quedó
undefined: el panel mostraba "no se pudo leer" SIEMPRE y "👤 ? a mano", **acusando de caído a
un sistema que andaba**. Y `fetchImportCandidates` dejó de devolver `url`, así que "Import N
URLs" abría **N pestañas en blanco**.
**How to apply:** al cambiar una función, revisar QUÉ CAMPOS lee cada llamador, no sólo que
compile.

## `.order()` en todo loop paginado
Sin orden explícito Postgres no garantiza el mismo orden entre dos consultas LIMIT/OFFSET, y
el cron escribe mientras se pagina: se repiten filas o **se saltean**. Una fila salteada en
`dominios-activos` es un dominio que debía estar bloqueado y no está.

## 🤝 Hay OTRA sesión de Claude en adeq-dashboard
`ListAgents` la muestra; se le habla con `SendMessage`. **Nunca `git add -A` en ese repo** —
arrastré tres archivos suyos en un commit mío. Commitear por nombre de archivo y avisar cuando
algo cruza. Coordinar sirvió: ellos arreglaron `LIVE` **de raíz** (`marcarLive` sólo promovía y
nunca degradaba) donde yo iba a poner un parche que habría tapado el síntoma.
**Preguntar antes de parchear el archivo de otro.**

Relacionado: [[reference_crm_board_modelo]], [[reference_monday_apagado]],
[[feedback_alertas_automaticas]]
