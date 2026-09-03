-- ═══════════════════════════════════════════════════════════════════════════════
--  Qué CLASE de prueba tenemos de que una dirección no sirve
-- ═══════════════════════════════════════════════════════════════════════════════
-- `toolbar_bounced_emails` guardaba cuatro cosas distintas como si fueran la misma, y estar
-- ahí bloquea una dirección PARA SIEMPRE:
--
--   rebote_smtp      el correo VOLVIÓ — la dirección no existe o nos bloquea      → bloquea
--   verificador      MillionVerifier antes de mandar: nunca salió, envío EVITADO  → bloquea
--   rebote_temporal  4xx: greylisting, buzón lleno → "ahora no", NO "nunca"       → NO bloquea
--   autorespuesta    un "estoy de vacaciones" PRUEBA que el buzón está vivo       → NO bloquea
--
-- Había 7 direcciones sanas bloqueadas de por vida, una de ellas sales@adeqmedia.com.

alter table toolbar_bounced_emails add column if not exists evidencia text;

-- ⚠️ Default explícito: `evidencia=in.(...)` de PostgREST NO matchea NULL, así que una fila
-- sin clasificar dejaría de estar bloqueada sin que nadie lo pida. La clase más conservadora
-- es la que bloquea: un rebote sin clasificar se trata como rebote.
alter table toolbar_bounced_emails alter column evidencia set default 'rebote_smtp';

update toolbar_bounced_emails set evidencia = case
  when reason ilike 'mv_%' or reason ilike '%millionverifier%'                       then 'verificador'
  when reason ilike '%auto_reply%'                                                   then 'autorespuesta'
  -- El gate ya dictaminó: si dijo que no existe o que nos bloquean, eso manda sobre el texto.
  when tipo in ('usuario_inexistente','dominio_inexistente','bloqueado')              then 'rebote_smtp'
  when reason ~* '(^|[^a-z])soft([^a-z]|$)'
    or coalesce(detalle,'') ~ '4[0-9][0-9][ -][0-9]\.[0-9]\.[0-9]'                    then 'rebote_temporal'
  else 'rebote_smtp' end
where evidencia is null;

alter table toolbar_bounced_emails alter column evidencia set not null;
alter table toolbar_bounced_emails drop constraint if exists toolbar_bounced_emails_evidencia_ck;
alter table toolbar_bounced_emails add constraint toolbar_bounced_emails_evidencia_ck
  check (evidencia in ('rebote_smtp','verificador','rebote_temporal','autorespuesta','sin_clasificar'));

create index if not exists idx_bounced_evidencia on toolbar_bounced_emails (evidencia);
