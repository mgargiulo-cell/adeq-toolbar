---
name: feedback-revision-03-09
description: "Lo que encontró la revisión con 5 agentes del 2026-09-03: 9 fallas, 7 introducidas por mí ese mismo día, y las reglas que salieron"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-03T09:18:52.773Z
---

Cinco agentes revisando en vivo un día de cambios grandes. **De 9 fallas reales, 7 las había
metido yo esa misma jornada.** Todas verificadas ejecutando, no leyendo.

## ⚠️⚠️ Una clave numérica en una respuesta del modelo se DESALINEA
Mi clasificador por lote pedía `{"1":"publisher","2":"..."}` usando el número de línea. Si el
modelo se saltea una línea **renumera todo lo que sigue** y cada dominio se lleva el veredicto
del siguiente. Medido: **entre 1 y 15 errores sobre 20 según la corrida** — intermitente, o sea
indetectable. Y permanente, porque `runSuspectRejectAnalysis` sella `suspect_checked_at`.
**La clave tiene que ser el DATO (el dominio), no su posición.** Así el corrimiento no existe:
o coincide o no se usa. Probado tras el cambio: 20/20.

⚠️ **Probé el lote con 6 sitios y dio 6/6.** Con 20 se rompe. **Una muestra de prueba más chica
que el uso real no prueba nada.**

⚠️ **El `page_title` viene scrapeado del sitio ajeno.** Sin sacarle `\n` y `|`, un sitio elegía
su propio `<title>` para meter una línea falsa en el lote: se colaba al pool Y hacía descartar
al que viniera después. Todo texto de terceros se limpia antes de entrar a un prompt.

## 💥 Errores míos que se disfrazaban de "no hay datos"
- **`order=sent_at` sobre una tabla sin esa columna** → HTTP 400 en el 100% de las llamadas.
  El puente de rebotes al CRM **nunca funcionó**, y el log decía *"no encuentro a qué sitio se
  lo mandamos"*. Un fallo de consulta y una falta de datos **no pueden verse iguales desde
  afuera**. (Y yo había visto la lista de columnas de esa tabla horas antes.)
- **`markEmailBounced`** logueaba "agregado al blocklist" sin mirar `res.ok`.
- **`_bloqueadosDelCrm`** fallaba abierto y mudo: null se lee como "no está bloqueado".
  ⚠️ Al arreglarlo casi lo empeoro: iba a devolver `null` con un snapshot viejo. **Una lista de
  60 h protege muchísimo más que ninguna** — se usa igual, pero gritando.

## 🔒 Dos agujeros de seguridad que abrí ese día
- **Archivar renombrando sacó las claves de Monday de su propia protección.** La policy
  comparaba por **nombre exacto**, y las variantes por usuario (`monday_api_key_<mail>`) nunca
  habían estado cubiertas. **Una lista de nombres exactos se rompe con cualquier renombre y el
  que renombra no se entera** → ahora filtra por patrón (`%api_key%`, `%secret%`, `%token%`).
- **`toolbar_claude_gasto` nació sin RLS**, la única `toolbar_*` sin protección, con la anon key
  viajando dentro del zip publicado. **Toda tabla nueva arranca con RLS, sin excepción.**

## 🚦 Interruptores y avisos que no hacían nada
- **`agent_manual_off` no lo leía NADIE**: el panel apaga el agente vaciando
  `agent_enabled_users`, y el "boot guarantee" la repoblaba en la vuelta siguiente. El comentario
  decía *"sin chequear horario ni manual_off"* como si fuera una simplificación. **El botón OFF
  no apagaba.** La garantía es para un vaciado ACCIDENTAL; un apagado deliberado no es un
  accidente.
- **`parteDelDia`: 9 días sin salir, cero `saludPing` en 800 líneas.** El vigilante mira 34 jobs
  y ése no era uno. **El aviso de que algo falló no puede depender de que ese algo funcione.**
- **`claude_daily_cap=0` no apagaba**: `Number(x) || 700` convierte el 0 en 700 — justo el valor
  que uno escribiría para frenar el gasto. Y vacío ≠ cero: `Number("")` es 0.
- **El gasto se anotaba en el día del VOLCADO.** El worker duerme de noche, así que la última
  tanda de cada tarde caía en el día siguiente **todos los días**, y eso solo garantizaba que el
  vigilante alertara sobre su propio desfase. **La fecha va en la clave, no se calcula al
  guardar.**

## 🔁 El orden importa: primero el efecto irreversible
`validateProspect` cargaba la ficha en el CRM **antes** de mandar el mail, y el `catch` sólo
pintaba el error. Si Gmail fallaba, el prospecto quedaba marcado como contactado y **no le
escribía nadie nunca** (`mail_ya_enviado` frena el inicial del CRM). El botón de Analysis ya lo
hacía bien. **Registrar que algo pasó va DESPUÉS de que pase.**

## 📋 Tercera vez: una lista paralela a un `<select>` diverge
La card de Prospects tenía su propia copia de estados con el vocabulario de Monday. Cinco de
ocho no existían: elegir "Mail No Enviado" guardaba "Propuesta Vigente" y arrancaba la cadencia
sobre alguien marcado como NO contactado. Ahora lee del propio `#form-estado`.

## Cómo pedir una revisión así
Agentes en paralelo, uno por área, con instrucción explícita de **probar en vivo** (consultar la
base, ejecutar la función, mandar el request real) y **prohibido escribir en producción**.
Pedirles también qué revisaron y está BIEN — ahorra re-auditar. Los hallazgos con comando +
salida; los que no traen evidencia, no se aplican sin verificarlos primero.

Relacionado: [[feedback_lecciones_migracion]], [[feedback_alertas_automaticas]],
[[reference_gasto_anthropic]], [[reference_lectura_rebotes]], [[project_security]]
