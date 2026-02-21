CREATE OR REPLACE FUNCTION pgpipe_notify_{{.Table}}() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('{{.Channel}}', json_build_object(
    'op', TG_OP,
    'table', TG_TABLE_NAME,
    'row', CASE WHEN TG_OP = 'DELETE' THEN row_to_json(OLD) ELSE row_to_json(NEW) END,
    'old', CASE WHEN TG_OP = 'UPDATE' THEN row_to_json(OLD) ELSE NULL END
  )::text);
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER pgpipe_{{.Table}}_trigger
  AFTER INSERT OR UPDATE OR DELETE ON {{.Table}}
  FOR EACH ROW EXECUTE FUNCTION pgpipe_notify_{{.Table}}();
