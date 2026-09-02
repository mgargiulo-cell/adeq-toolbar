-- ═══════════════════════════════════════════════════════════════════════════════════════
-- BOTÓN DE RESTABLECER — suelta TODOS los frenos de una (Maxi 2026-09-02, pedido del user)
-- ═══════════════════════════════════════════════════════════════════════════════════════
-- El panel ya tenía cómo soltar el kill switch, pero ese no es el único freno. Cuando el
-- agente aparece en "0 de 40" casi nunca es el kill switch: es `agent_paused_until`, la pausa
-- de 12 h por rebote del dominio. Son dos llaves distintas y el user solo podía tocar una.
--
-- El caso que lo motivó: el 02/09 el agente llevaba el día entero en cero con el kill switch
-- en `false`. La pausa vencía recién 11 horas después y, al vencer, se volvía a disparar
-- porque la tasa de rebote se mide sobre 7 días: frenar los envíos vacía el denominador y
-- empuja la tasa PARA ARRIBA. El freno se garantizaba a sí mismo y no había forma de salir
-- desde la toolbar.
--
-- Este RPC suelta las dos, deja registro de quién y cuándo, y NO desarma la defensa: si la
-- causa sigue, el vigilante vuelve a frenar en la próxima vuelta. Es una salida de
-- emergencia, no un interruptor para dejar apagado.
CREATE OR REPLACE FUNCTION restablecer_operacion(p_motivo text DEFAULT 'desde el panel')
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_email text;
  v_soltados text[] := '{}';
  v_pausa   text;
  v_kill    text;
BEGIN
  -- Misma validación que toggle_kill_switch: solo la allowlist puede tocar esto.
  v_email := lower(coalesce(auth.jwt() ->> 'email', ''));
  IF v_email = '' OR NOT EXISTS (
        SELECT 1 FROM toolbar_config
        WHERE key = 'security_allowlist' AND lower(value) LIKE '%' || v_email || '%')
  THEN
    RAISE EXCEPTION 'no autorizado';
  END IF;

  SELECT value INTO v_kill  FROM toolbar_config WHERE key = 'kill_switch';
  SELECT value INTO v_pausa FROM toolbar_config WHERE key = 'agent_paused_until';

  IF coalesce(v_kill,'') = 'true' THEN
    UPDATE toolbar_config SET value = 'false' WHERE key = 'kill_switch';
    UPDATE toolbar_config SET value = ''      WHERE key IN ('kill_switch_reason','kill_switch_auto_at');
    v_soltados := v_soltados || 'kill_switch';
  END IF;

  -- La pausa por rebote: se limpia solo si está vigente, para no ensuciar el registro al pedo.
  IF v_pausa IS NOT NULL AND v_pausa <> '' AND v_pausa::timestamptz > now() THEN
    UPDATE toolbar_config SET value = '' WHERE key = 'agent_paused_until';
    v_soltados := v_soltados || 'pausa_por_rebote';
  END IF;

  -- Las cuotas de APIs (Apollo, RapidAPI, SimilarWeb, Serper, MillionVerifier) NO se tocan:
  -- son contadores de gasto real del día, no frenos de seguridad. Ponerlos en cero desde acá
  -- sería gastar plata de verdad creyendo que se está soltando un freno.
  INSERT INTO toolbar_security_events (kind, severity, actor, detail, created_at)
  VALUES ('operacion_restablecida', 'warn', v_email,
          jsonb_build_object('motivo', p_motivo, 'soltados', v_soltados), now());

  RETURN jsonb_build_object(
    'ok', true,
    'soltados', v_soltados,
    'nada_que_soltar', (array_length(v_soltados,1) IS NULL)
  );
END;
$$;

REVOKE ALL ON FUNCTION restablecer_operacion(text) FROM public, anon;
GRANT EXECUTE ON FUNCTION restablecer_operacion(text) TO authenticated;

COMMENT ON FUNCTION restablecer_operacion IS
  'Suelta de una el kill switch y la pausa por rebote. No desarma la defensa: si la causa '
  'sigue, el vigilante vuelve a frenar. Solo la allowlist puede ejecutarla.';
