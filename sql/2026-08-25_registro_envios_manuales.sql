-- ══════════════════════════════════════════════════════════════════════════════
-- QUE EL ENVÍO MANUAL DEJE RASTRO — 2026-08-25
--
-- `toolbar_agent_actions` tenía UNA sola policy: SELECT. Ninguna de INSERT.
-- O sea que `createManualSendTracking` (modules/supabase.js:259), que el popup
-- llama antes de cada envío manual, venía siendo rechazada por RLS desde
-- siempre. El popup traga el error con un console.warn y manda el mail igual.
--
-- Lo que costaba:
--   1. CERO registro de quién mandó qué a mano. Las 2.064 filas `action='sent'`
--      son todas del worker (service role) — o sea, del agente. Por eso no se
--      puede responder "cuántos mails mandó cada MB ayer".
--   2. El píxel de open NO se inyecta: el popup necesita el `id` de la fila para
--      armar la URL del track-open. Sin fila no hay píxel, y sin píxel el
--      "Email Futuro" (reenganche a los 11 días si no abrió) nunca se dispara
--      en los envíos manuales.
--
-- La policy es ESTRECHA a propósito: cada quien puede insertar SOLO filas a su
-- propio nombre. No puede escribir a nombre de otro MB ni tocar lo del agente
-- (que entra por service role y se saltea RLS).
-- ══════════════════════════════════════════════════════════════════════════════

-- ── 1 · VER CÓMO ESTÁ ANTES ──────────────────────────────────────────────────
SELECT polname, polcmd::text AS cmd
  FROM pg_policy p JOIN pg_class c ON c.oid = p.polrelid
 WHERE c.relname = 'toolbar_agent_actions';


-- ── 2 · LA POLICY QUE FALTABA ────────────────────────────────────────────────
DROP POLICY IF EXISTS toolbar_agent_actions_self_insert ON toolbar_agent_actions;

CREATE POLICY toolbar_agent_actions_self_insert
  ON toolbar_agent_actions
  FOR INSERT
  TO authenticated
  WITH CHECK (user_email = (auth.jwt() ->> 'email'));


-- La fila se inserta ANTES de mandar el mail (el píxel de open necesita su id). Si
-- Gmail después rechaza el envío, hay que poder corregirla: si no, queda un "sent"
-- fantasma que infla el parte diario. Igual de estrecha: solo las filas propias.
DROP POLICY IF EXISTS toolbar_agent_actions_self_update ON toolbar_agent_actions;

CREATE POLICY toolbar_agent_actions_self_update
  ON toolbar_agent_actions
  FOR UPDATE
  TO authenticated
  USING      (user_email = (auth.jwt() ->> 'email'))
  WITH CHECK (user_email = (auth.jwt() ->> 'email'));


-- ── 3 · CHEQUEO ──────────────────────────────────────────────────────────────
-- Tienen que aparecer las dos: la de lectura y la de inserción.
SELECT polname, polcmd::text AS cmd,
       coalesce(pg_get_expr(polwithcheck, polrelid), '-') AS with_check
  FROM pg_policy p JOIN pg_class c ON c.oid = p.polrelid
 WHERE c.relname = 'toolbar_agent_actions'
 ORDER BY polname;

-- A partir del próximo envío manual desde la toolbar tiene que aparecer acá:
--   SELECT user_email, count(*) FROM toolbar_agent_actions
--    WHERE action='sent' AND details->>'ui_origin'='toolbar_manual'
--    GROUP BY 1;
