
-- dim_date
CREATE TABLE IF NOT EXISTS dim_date (
  date        date PRIMARY KEY,
  year        int NOT NULL,
  month       int NOT NULL,
  day         int NOT NULL,
  iso_week    int NOT NULL,
  iso_dow     int NOT NULL,   -- 1=Mon..7=Sun
  quarter     int NOT NULL,
  built_at    timestamptz NOT NULL DEFAULT now()
);