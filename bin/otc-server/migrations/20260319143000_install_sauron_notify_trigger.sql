CREATE OR REPLACE FUNCTION public.sauron_watch_set_notify()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  target_swap_id uuid;
  payload text;
BEGIN
  target_swap_id := COALESCE(NEW.id, OLD.id);
  payload := json_build_object('swapId', target_swap_id::text)::text;
  PERFORM pg_notify('sauron_watch_set_changed', payload);
  RETURN COALESCE(NEW, OLD);
END;
$$;

DROP TRIGGER IF EXISTS sauron_watch_set_changed_notify ON public.swaps;

CREATE TRIGGER sauron_watch_set_changed_notify
AFTER INSERT OR UPDATE OR DELETE ON public.swaps
FOR EACH ROW
EXECUTE FUNCTION public.sauron_watch_set_notify();

ALTER TABLE public.swaps
ENABLE ALWAYS TRIGGER sauron_watch_set_changed_notify;
