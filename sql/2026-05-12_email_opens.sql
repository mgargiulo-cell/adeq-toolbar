-- Open-rate tracking via tracking pixel.
-- Cada email del agente inyecta <img src="track-open?aid=AGENT_ACTION_ID" />.
-- La Edge Function `track-open` devuelve 1x1 PNG y graba aquí.

CREATE TABLE IF NOT EXISTS toolbar_email_opens (
  id              bigserial PRIMARY KEY,
  agent_action_id bigint NOT NULL REFERENCES toolbar_agent_actions(id) ON DELETE CASCADE,
  opened_at       timestamptz NOT NULL DEFAULT now(),
  user_agent      text,
  ip_hash         text  -- hash de IP, no IP cruda (privacy)
);

CREATE INDEX IF NOT EXISTS idx_email_opens_agent_action
  ON toolbar_email_opens (agent_action_id);
CREATE INDEX IF NOT EXISTS idx_email_opens_opened_at
  ON toolbar_email_opens (opened_at DESC);

-- RLS: insert público (la Edge Function usa service role); select solo admin
ALTER TABLE toolbar_email_opens ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS opens_admin_select ON toolbar_email_opens;
CREATE POLICY opens_admin_select ON toolbar_email_opens
  FOR SELECT USING (auth.jwt() ->> 'email' = 'mgargiulo@adeqmedia.com');

-- View con open-rate por subject template + por language
CREATE OR REPLACE VIEW toolbar_agent_open_rate AS
SELECT
  a.pitch_subject,
  a.details->>'language' AS language,
  a.details->>'source'   AS source,
  count(DISTINCT a.id)                                                AS total_sent,
  count(DISTINCT o.agent_action_id)                                   AS total_opened,
  round(100.0 * count(DISTINCT o.agent_action_id) / NULLIF(count(DISTINCT a.id), 0), 1) AS open_rate_pct
FROM toolbar_agent_actions a
LEFT JOIN toolbar_email_opens o ON o.agent_action_id = a.id
WHERE a.action = 'sent'
  AND a.created_at > now() - interval '60 days'
GROUP BY a.pitch_subject, a.details->>'language', a.details->>'source'
HAVING count(DISTINCT a.id) >= 3   -- mínimo 3 envíos para promediar
ORDER BY open_rate_pct DESC NULLS LAST;

NOTIFY pgrst, 'reload schema';
