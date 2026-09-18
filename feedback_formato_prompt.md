---
name: feedback-formato-prompt
description: "Todo lo que sea para pasarle a la otra IA (la sesión del dashboard) va en formato prompt, listo para copiar y pegar"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: d0a1004c-ed72-4de6-9d83-21ea76e7336a
  modified: 2026-09-03T02:01:01.698Z
---

Pedido del user (2026-09-03): *"Lo que me tengas que dar de indicaciones dámelo en formato
prompt siempre"*.

Cuando el resultado de algo sea **algo que el user le tiene que decir a la sesión del
dashboard**, no se lo explico a él para que lo traduzca: se lo doy en un bloque de código,
escrito **en segunda persona hacia la otra IA**, listo para pegar sin editar.

**Why:** el user es el cable entre las dos sesiones. Si le doy un análisis, tiene que
convertirlo él en instrucción — trabajo extra y una oportunidad de que se pierda un detalle
técnico (una línea, un nombre de columna, una evidencia).

**How to apply:**
- Va **dentro de un bloque de código**, para que se copie de una.
- **En segunda persona hacia la otra IA**, no hacia el user.
- Autocontenido: la otra sesión no tiene mi contexto. Nombre de archivo, línea, la consulta
  que lo comprueba, y el POR QUÉ importa.
- Si hay una decisión tomada, va **primero y sin rodeos**.
- Lo que le toca a él (aprobar, cancelar algo en un panel, decidir) va **fuera** del bloque,
  en texto normal — dentro va sólo lo que le habla a la otra IA.

Ver [[reference_arquitectura]] y el reparto en `PROMPT-DASHBOARD.md`.
