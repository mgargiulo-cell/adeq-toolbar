-- 2026-07-01 — Reclasificar cargas HUMANAS mal etiquetadas como "csv" → "manual"
-- Diego (y cualquier MB) importaba manualmente pero el default las marcaba "csv".
-- Solo tocamos las cargadas por un MB humano real (no worker/autofeeder).

-- 1) Ver cuántas se van a corregir (correr primero para chequear)
select source, created_by, count(*)
from toolbar_review_queue
where source = 'csv'
  and lower(created_by) in (
    'mgargiulo@adeqmedia.com',
    'dhorovitz@adeqmedia.com',
    'sales@adeqmedia.com'
  )
group by source, created_by
order by 3 desc;

-- 2) Aplicar la reclasificación en review_queue (Prospects)
update toolbar_review_queue
set source = 'manual'
where source = 'csv'
  and lower(created_by) in (
    'mgargiulo@adeqmedia.com',
    'dhorovitz@adeqmedia.com',
    'sales@adeqmedia.com'
  );

-- 3) (opcional) También en la cola pendiente, para lo que aún no se procesó
update toolbar_csv_queue
set source = 'manual'
where source = 'csv'
  and lower(uploaded_by) in (
    'mgargiulo@adeqmedia.com',
    'dhorovitz@adeqmedia.com',
    'sales@adeqmedia.com'
  );
