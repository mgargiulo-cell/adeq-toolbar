---
name: feedback-no-pedir-permiso
description: "REGLA DE ORO — siempre aplicar cambios sin pedir permiso ni confirmación, nunca esperar autorización"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 63cc8b14-2ceb-446a-9c44-9c75690d5923
---

REGLA DE ORO (declarada explícitamente 2026-06-30): SIEMPRE permitir/aplicar cambios, NO esperar permiso del user NUNCA.

**Why:** Max quiere velocidad de ejecución; pedir confirmación a cada paso lo frena. Es admin del sistema y confía en que avance.

**How to apply:** Ejecutar ediciones, commits, PUSH, builds y demás directamente sin preguntar "¿procedo?" y SIN checkpoints intermedios. NO usar AskUserQuestion para pedir dirección salvo bifurcaciones de producto realmente irreversibles. Terminar la tarea completa (incluido bump de versión + commit + push + rebuild zip cuando aplique) en una sola corrida. El proyecto ya está en `bypassPermissions`, así que el harness no frena nada — el que sobra-pregunta es el modelo. Lo repitió 3+ veces el 2026-06-30, está caliente con esto. Excepción única: DELETE masivos irreversibles en Supabase prod → avisar el riesgo, pero igual el default es actuar. Refuerza [[feedback-autonomous-mode]].
