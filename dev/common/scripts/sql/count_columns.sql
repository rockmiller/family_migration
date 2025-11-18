SELECT COUNT(*) 
FROM information_schema.columns 
WHERE table_name = 'cr1880' 
  AND table_schema = 'family';  -- or your actual schema