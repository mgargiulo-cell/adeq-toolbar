# Ver en qué se gasta la key de Anthropic

Todo lo de acá empezó a registrarse el **2026-09-03**. Los días anteriores tienen el total
(lo contaba el proxy) pero no el desglose, porque no existía.

## Las tres consultas

### 1. ¿En qué se gastó? — requests, tokens y plata, por motivo
```sql
select dia, fuente, motivo, modelo, llamadas, entrada, salida, usd_aprox
from toolbar_gasto_claude_vista
where dia >= current_date - 7
order by dia desc, usd_aprox desc;
```
`llamadas` = requests · `entrada`/`salida` = tokens · `usd_aprox` = estimado.

⚠️ **Mirá la columna de plata, no la de requests.** Una request de Sonnet cuesta un orden de
magnitud más que una de Haiku: medido, 8 de Sonnet salen **8,5 veces** más que 12 de Haiku.
El ranking por requests y el ranking por plata no dan el mismo orden.

### 2. ¿Entiendo todo lo que se gastó?
```sql
select * from toolbar_gasto_claude_cruce order by dia desc limit 15;
```
`proxy` es la autoridad (cuenta **todo** lo que pasa por él). `con_motivo` es lo que tengo
desglosado. **`sin_explicar` es el número que importa**: son requests que salieron por un
camino que no estoy viendo, o una puerta que dejó de anotar. Mientras no sea ~0, cualquier
conclusión sobre en qué se gasta es parcial.

### 3. ¿Esto es normal?
```sql
select motivo, fuente,
       round(avg(llamadas)) promedio, max(llamadas) pico,
       round(sum(usd_aprox)::numeric, 2) usd_total
from toolbar_gasto_claude_vista
where dia >= current_date - 14
group by motivo, fuente order by usd_total desc;
```
Con 3+ días de historia esto ya sirve para ver qué motivo se salió de su rango.

## Lo que avisa solo

- **El boletín de salud** trae una sección con el gasto del día anterior, encabezada por el
  motivo **más caro** (no el más frecuente — para optimizar no es lo mismo).
- **`vigilarGastoClaude`** corre todos los días después de las 9 Madrid y alerta si:
  - más del **5%** de las requests no tienen motivo → hay un camino que no veo;
  - algún motivo hizo **3× su mediana** y al menos 50 requests más → se disparó.
    El umbral sale de la historia de **cada motivo**, no de un número fijo: un tope fijo con
    motivos de 10 requests y otros de 1.500 o no salta nunca o salta siempre.
  - Pinguea **aunque no encuentre nada** y dice cuántos días de base tiene: "0 anomalías" con
    un día de historia no significa nada.

## Las dos puertas (verificado en todo el repo)

| Dónde | Quién | Motivos |
|---|---|---|
| `auto-prospector/index.js` → `llamarClaude()` | worker | `clasificar_sitio_lote`, `clasificar_sitio`, `sitio_bloqueado`, `idioma_envio`, `idioma_deteccion`, `email_pick`, `pitch`, `reglas_pitch`, `reglas_descarte`, `keywords`, `suspect_reject` |
| `modules/claude.js` → `callClaude()` | extensión | `pitch_a_pedido` ⚡, `analisis_revenue` ⚡, `tipo_de_web` |

⚡ = Sonnet (lo caro). Todo lo demás es Haiku.
Si aparece el motivo **`sin_etiquetar`**, es una llamada nueva de la extensión que alguien
agregó sin ponerle nombre — hay que etiquetarla.

## Contexto para leer los números

- **Casi todo el gasto es el worker.** Las personas usaron Claude **175 veces desde el 22/04**,
  y la última fue el 26/08: el botón "generar" casi no se toca porque hoy se mandan plantillas.
- **El worker gastaba 0 hasta el 25/08.** No porque fuera más barato: **estaba trabado**. Se
  destrabó el 26 y ahí empezó a consumir. El gasto es trabajo real.
- **Techo diario**: `claude_daily_cap` (hoy 700), en `toolbar_config`. No corta lo que va en el
  camino de un envío (`pitch`, `email_pick`, `idioma_envio`); sólo hace esperar al
  enriquecimiento. Al cruzarlo avisa una vez.
- Los **precios** viven en la vista `toolbar_gasto_claude_vista`, no en el código, para poder
  corregirlos sin deployar.
