
-- ═══════════════════════════════════════════════════════════════════════════════════════
-- EL AVISO DE "TU COMPAÑERO ESTÁ EN ESTE PROSPECTO" DURABA 30 MINUTOS (Maxi 2026-09-02)
-- ═══════════════════════════════════════════════════════════════════════════════════════
-- El user: "no pasa que dos al mismo tiempo estén prospectándolos... que salga el aviso si tu
-- compañero lo abrió en los últimos 2 minutos. Pero no quiero seguir agregando crons".
--
-- Dos problemas encontrados:
--  1. El bloqueo se toma al ABRIR LA TOOLBAR en cualquier página, no al empezar a prospectar.
--     Por eso había bloqueos sobre `chatgpt.com` y `adeqmedia.monday.com`, que no son
--     prospectos: alguien tenía esas pestañas abiertas.
--  2. `unlockProspect` existe pero el flujo normal NO lo llama: el bloqueo se suelta solo al
--     vencer. Con 30 minutos, abrir una pestaña dejaba media hora de "está ocupado".
--
-- El arreglo va acá y no en la extensión a propósito: cambiar el DEFAULT del RPC surte efecto
-- al instante para todos, incluidos los MB que todavía tienen la versión vieja de la tienda.
-- 2 minutos alcanzan para el caso real —dos personas abriendo el mismo sitio a la vez— y si
-- alguien cierra la pestaña, el aviso se va solo enseguida.
CREATE OR REPLACE FUNCTION public.lock_prospect(p_domain text, p_email text, p_minutes integer DEFAULT 2)
RETURNS TABLE(ok boolean, locked_by text, expires_at timestamptz)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_existing  public.toolbar_prospect_locks%ROWTYPE;
  v_expires   timestamptz := now() + (p_minutes || ' minutes')::interval;
BEGIN
  -- Limpieza oportunista, sin cron: cada vez que alguien toma un bloqueo se barren los
  -- vencidos de hace más de un día. Es un DELETE que casi siempre matchea cero filas, y
  -- evita que la tabla crezca para siempre (tenía 3.986 vencidos sin borrar).
  -- ⚠️ Calificar la columna: `expires_at` es TAMBIÉN el nombre de la salida del RETURNS TABLE,
  -- y sin el prefijo Postgres tira "column reference is ambiguous" y el bloqueo deja de
  -- funcionar del todo. Lo agarró la prueba, no la lectura.
  DELETE FROM public.toolbar_prospect_locks l WHERE l.expires_at < now() - interval '1 day';

  SELECT * INTO v_existing FROM public.toolbar_prospect_locks l WHERE l.domain = p_domain;

  IF v_existing.domain IS NOT NULL
     AND v_existing.locked_by <> p_email
     AND v_existing.expires_at > now()
  THEN
    RETURN QUERY SELECT false, v_existing.locked_by, v_existing.expires_at;
    RETURN;
  END IF;

  INSERT INTO public.toolbar_prospect_locks(domain, locked_by, locked_at, expires_at)
  VALUES (p_domain, p_email, now(), v_expires)
  ON CONFLICT (domain) DO UPDATE
    SET locked_by = excluded.locked_by,
        locked_at = excluded.locked_at,
        expires_at = excluded.expires_at;

  RETURN QUERY SELECT true, p_email, v_expires;
END;
$$;
