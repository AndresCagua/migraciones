-- Script para generar datos de prueba en PostgreSQL
-- Genera 10,000 clientes y aproximadamente 30,000 pedidos

-- 1. Limpiar tablas existentes (opcional)
TRUNCATE TABLE public.clientes CASCADE;
TRUNCATE TABLE public.pedidos CASCADE;

-- 2. Función para generar strings aleatorios
CREATE OR REPLACE FUNCTION random_string(length integer) RETURNS TEXT AS $$
DECLARE
  chars TEXT := 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
  result TEXT := '';
  i INTEGER := 0;
BEGIN
  FOR i IN 1..length LOOP
    result := result || substr(chars, (random() * length(chars) + 1)::INTEGER, 1);
  END LOOP;
  RETURN result;
END;
$$ LANGUAGE plpgsql;

-- 3. Función para generar emails aleatorios
CREATE OR REPLACE FUNCTION random_email() RETURNS TEXT AS $$
BEGIN
  RETURN random_string(8) || '@' || 
         CASE WHEN random() < 0.3 THEN 'gmail' 
              WHEN random() < 0.6 THEN 'hotmail' 
              ELSE 'yahoo' END || 
         CASE WHEN random() < 0.7 THEN '.com' 
              WHEN random() < 0.9 THEN '.net' 
              ELSE '.org' END;
END;
$$ LANGUAGE plpgsql;

-- 4. Función para generar fechas aleatorias
CREATE OR REPLACE FUNCTION random_date(start_date DATE, end_date DATE) RETURNS DATE AS $$
BEGIN
  RETURN start_date + (random() * (end_date - start_date + 1))::INTEGER;
END;
$$ LANGUAGE plpgsql;

-- 5. Insertar 10,000 clientes
-- Primero asegurémonos que las funciones auxiliares existan
-- Ahora el bloque DO corregido
DO $$
DECLARE
  i INTEGER;
  ciudades TEXT[] := ARRAY['Lima', 'Arequipa', 'Trujillo', 'Chiclayo', 'Piura', 'Cusco', 'Iquitos', 'Huancayo', 'Tacna', 'Puno'];
  paises TEXT[] := ARRAY['Perú', 'Chile', 'Colombia', 'Ecuador', 'Bolivia', 'Argentina', 'Brasil', 'México', 'EE.UU.', 'España'];
  random_num FLOAT;
  array_index INTEGER;
BEGIN
  FOR i IN 1..100 LOOP
    -- Generar números aleatorios una vez para evitar repeticiones
    random_num := random();
    array_index := (random_num * array_length(ciudades, 1))::INTEGER + 1;
    
    INSERT INTO public.clientes (
      nombre, 
      apellido, 
      email, 
      telefono, 
      fecha_nacimiento, 
      direccion, 
      ciudad, 
      pais, 
      codigo_postal
    ) VALUES (
      'Cliente' || i,
      'Apellido' || (random() * 1000)::INTEGER,
      random_email(),
      '9' || (90000000 * random() + 10000000)::INTEGER::TEXT,
      random_date('1950-01-01', '2005-12-31'),
      'Calle ' || (100 * random())::INTEGER::TEXT || ' #' || (1000 * random())::INTEGER::TEXT,
      ciudades[array_index],
      paises[(random() * array_length(paises, 1))::INTEGER + 1],
      (10000 * random())::INTEGER::TEXT
    );
    
    -- Mostrar progreso cada 1000 registros
    IF i % 1000 = 0 THEN
      RAISE NOTICE 'Insertados % clientes', i;
    END IF;
  END LOOP;
END $$;


-- 6. Insertar pedidos para los clientes (aprox. 3 pedidos por cliente)
DO $$
DECLARE
  cliente RECORD;
  num_pedidos INTEGER;
  j INTEGER;
  estados TEXT[] := ARRAY['pendiente', 'en_proceso', 'completado', 'cancelado'];
  metodos_pago TEXT[] := ARRAY['Tarjeta', 'Transferencia', 'Efectivo', 'PayPal'];
BEGIN
  FOR cliente IN SELECT id_cliente FROM public.clientes ORDER BY id_cliente LOOP
    -- Cada cliente tiene entre 1 y 5 pedidos
    num_pedidos := floor(random() * 5) + 1;
    
    FOR j IN 1..num_pedidos LOOP
      INSERT INTO public.pedidos (
        id_cliente,
        fecha_pedido,
        fecha_entrega,
        estado,
        total,
        metodo_pago,
        direccion_envio,
        notas
      ) VALUES (
        cliente.id_cliente,
        random_date('2020-01-01', CURRENT_DATE),
        CASE WHEN random() < 0.8 THEN random_date('2020-01-01', CURRENT_DATE + 30) ELSE NULL END,
        estados[floor(random() * array_length(estados, 1)) + 1],
        (random() * 1000 + 10)::NUMERIC(10,2),
        metodos_pago[floor(random() * array_length(metodos_pago, 1)) + 1],
        'Calle ' || floor(random() * 100)::TEXT || ' #' || floor(random() * 1000),
        CASE WHEN random() < 0.3 THEN 'Urgente' 
             WHEN random() < 0.5 THEN 'Regalo' 
             WHEN random() < 0.7 THEN 'Fragil' 
             ELSE NULL END
      );
    END LOOP;
  END LOOP;
  
  RAISE NOTICE 'Insertados todos los pedidos';
END $$;

-- 7. Estadísticas finales
SELECT 
  (SELECT COUNT(*) FROM public.clientes) AS total_clientes,
  (SELECT COUNT(*) FROM public.pedidos) AS total_pedidos,
  (SELECT AVG(total) FROM public.pedidos) AS promedio_monto_pedido,
  (SELECT MIN(fecha_pedido) FROM public.pedidos) AS primer_pedido,
  (SELECT MAX(fecha_pedido) FROM public.pedidos) AS ultimo_pedido;

-- 8. Limpiar funciones temporales (opcional)
DROP FUNCTION IF EXISTS random_string(integer);
DROP FUNCTION IF EXISTS random_email();
DROP FUNCTION IF EXISTS random_date(DATE, DATE);
