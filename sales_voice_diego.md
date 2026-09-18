---
name: ADEQ Style (Publishers Relations)
description: Estilo ADEQ para outreach a publishers — sin firma personal, válido para todas las cuentas
type: project
originSessionId: b9fd9a24-e6b6-4c02-b4f9-21155b7bf17e
---
## Contexto
Las constantes históricamente llamadas "voz Diego" se rebrandearon a **estilo ADEQ** (commit 0e385c3, 2026-05-12). El user explícitamente pidió que NINGÚN mail mencione "Diego" — es un estilo único para todas las cuentas (Diego, Agus, Max, etc.).

## Reglas inmutables

### Apertura
- SIEMPRE: `Hola [Nombre], ¿cómo estás?` (con nombre si lo hay)
- Inglés: `Hi [Name], how are you?` o `Hi [Name], how have you been?`
- Sin nombre: `Hola, ¿cómo estás?`

### Firma — REGLA DURA
- **JAMÁS firma con nombre propio del equipo ADEQ** (ni "Diego", ni "Max", ni "Agus")
- La firma del Gmail (configurada en Google Workspace) se appendea automáticamente
- Cierre del cuerpo: pregunta concreta + opcional "Saludos."

### Largo del primer mail (mail inicial agente)
- 50-100 palabras MAX (worker `generatePitchAgent`)
- Mail manual del MB humano: 60-130 palabras
- Sin bullets, sin negritas, sin formato — solo párrafos cortos (1-2 líneas c/u)

### CPM mencionable
- Slider/corner: 1 USD fijo ✓
- Video instream: 1.5-2.5 USD + 50% fillrate ✓
- Header bidding: SOLO uplift 25-30%, NUNCA CPM puntual
- Display puro: NUNCA dar CPM (depende GEO+vertical)

### Auto-conciencia del spam (anzuelo)
- "Se que te llegan 10/20 mails al día"
- "Intento ser breve"
- "Para no perder tiempo ambos"

### Argumento transversal
- "No tenemos cláusulas de exclusividad ni períodos mínimos de permanencia"
- "Sumamos al stack que ya tenés, no reemplazamos nada"
- "Solo seguimos si los resultados son buenos"

### Cierre
SIEMPRE pregunta concreta sobre charlar/probar. Variantes:
- ¿Cómo lo ves?
- ¿Qué opinás?
- ¿Te interesa probar?
- Espero tu aviso

### Idiomas (estrategia)
- Castellano: voseo si AR/UY, tuteo si MX/CO/ES
- Inglés: cordial, sin presión LATAM
- PT-BR: "Tudo bem?" + Abraços
- PT-PT: más formal, Cumprimentos
- Árabe: traductor formal + mencionar Raialyoum
- Italiano/Polaco/Búlgaro: responder en INGLÉS
- Francia/Alemania: NO prospectar activamente

### Clientes referencia válidos (NO inventar otros)
- Raialyoum.com — árabe / MENA
- Ciclo21.com — news ES
- MuchoDeporte.com — sports ES
- Footballia.net — sports
- ElPilon.com.co — news Colombia
- owngoalnigeria.com / zamusic.co.za / fakazahub.com — África

## Implementación técnica
- **Storage**: Supabase `toolbar_user_prompts` con `user_email = '__global__'`
- **Fallback baked**: `modules/diegoVoicePrompt.js` (export `ADEQ_STYLE_PROMPT`, alias `DIEGO_VOICE_PROMPT` para compat) y `auto-prospector/index.js` (`ADEQ_STYLE_FALLBACK`)
- **Worker agente**: `generatePitchAgent` llama `_getAdeqStyle(token)` con cache 30min
- **Popup MB humano**: `gemini.js` inyecta el prompt como **PROMPT MAESTRO** con triple refuerzo
- **Split 80/20**: 80% template (sin Claude, baked en `auto-prospector/templates.js`), 20% Claude. Configurable via `toolbar_config.agent_claude_percent`

## Decisión histórica
El user (Max, CEO) calibró el estilo con emails reales 2024-2026 de Diego Horovitz, pero el rebrand 2026-05-12 desacopló el estilo del nombre — ahora es propiedad del equipo ADEQ Publishers Relations.
