ALTER TABLE public.dvf ADD section_prefixe VARCHAR(5);
UPDATE public.dvf SET section_prefixe = SUBSTRING(id_parcelle, 6, 5);