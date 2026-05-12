-- Bounce detection: emails que el SMTP rechazó.
-- El worker escanea INBOX por mensajes de mailer-daemon y los marca acá.
-- rankEmail/scrapeEmailsForDomain consultan esta tabla como filtro adicional.

CREATE TABLE IF NOT EXISTS toolbar_bounced_emails (
  email           text PRIMARY KEY,                    -- email destino que bounceó
  bounced_at      timestamptz NOT NULL DEFAULT now(),
  reason          text,                                -- "550 No such user", "DNS failure", etc.
  original_action_id  bigint REFERENCES toolbar_agent_actions(id) ON DELETE SET NULL,
  original_domain text,                                -- dominio del lead que originó el bounce
  retry_attempted boolean NOT NULL DEFAULT false       -- ya intentamos email alternativo del lead?
);

CREATE INDEX IF NOT EXISTS idx_bounced_emails_domain ON toolbar_bounced_emails (original_domain);
CREATE INDEX IF NOT EXISTS idx_bounced_emails_retry  ON toolbar_bounced_emails (retry_attempted) WHERE retry_attempted = false;

ALTER TABLE toolbar_bounced_emails ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS bounced_read_all ON toolbar_bounced_emails;
CREATE POLICY bounced_read_all ON toolbar_bounced_emails
  FOR SELECT TO authenticated USING (true);

DROP POLICY IF EXISTS bounced_insert_authenticated ON toolbar_bounced_emails;
CREATE POLICY bounced_insert_authenticated ON toolbar_bounced_emails
  FOR INSERT TO authenticated WITH CHECK (true);

DROP POLICY IF EXISTS bounced_update_authenticated ON toolbar_bounced_emails;
CREATE POLICY bounced_update_authenticated ON toolbar_bounced_emails
  FOR UPDATE TO authenticated USING (true);

NOTIFY pgrst, 'reload schema';
