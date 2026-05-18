-- ============================================================
-- Agent: % de pitches generados por Claude vs templates.
-- Default: 80% template (de los 3 baked validados + drafts DB en pool
-- ponderado por open rate) / 20% Claude.
--
-- Update 2026-05-18: user prefiere 80/20 (template/IA) en vez del 50/50
-- que estaba probando. Templates ya son sólidos post-validación.
-- ============================================================

insert into public.toolbar_config (key, value)
values ('agent_claude_percent', '20')
on conflict (key) do update
  set value = excluded.value;
