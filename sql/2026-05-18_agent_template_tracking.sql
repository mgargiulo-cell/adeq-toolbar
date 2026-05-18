-- ============================================================
-- Template tracking en agent_actions
--
-- Cada acción 'sent' del Agent loggea qué template usó (baked o DB draft).
-- Con esto + toolbar_email_opens podemos calcular open_rate por template
-- y ponderar la selección random: templates con más opens reciben más
-- probabilidad de ser elegidos en futuros envíos.
--
-- IDs:
--   baked_<lang>_<idx>   → templates.js (índice 0/1/2 por idioma)
--   db_<id>              → toolbar_pitch_drafts (serial id)
-- ============================================================

alter table public.toolbar_agent_actions
  add column if not exists template_id text;

create index if not exists idx_agent_actions_template_id
  on public.toolbar_agent_actions(template_id)
  where template_id is not null;

-- Índice combinado para la query de scoring:
-- "para action=sent en los últimos 30d, agrupar por template_id"
create index if not exists idx_agent_actions_sent_template
  on public.toolbar_agent_actions(action, created_at desc, template_id)
  where action = 'sent';
