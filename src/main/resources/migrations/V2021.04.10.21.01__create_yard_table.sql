-- Create initial tables.

CREATE TABLE IF NOT EXISTS public.yard (
          Id serial not null,
          Color varchar,
          Warehouse varchar,
          assignation_number integer,
          constraint "PK_Yard" primary key(ID)
);