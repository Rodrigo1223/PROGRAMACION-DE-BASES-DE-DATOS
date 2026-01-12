-- =============================================================================
-- TRABAJO SEMANA 1 - PRY2206
-- DESARROLLO CASOS 1 Y 2
-- =============================================================================
/* NOTA:
  Al revisar las tablas, me di cuenta de que los RUT 
  que venían en la guía no coincidían con los que cargó
  el script de datos. Para que los bloques funcionen
  y muestren los cálculos, usé los RUTs que sí tienen 
  información registrada en las tablas de créditos."
*/

-- Primero ajusto las fechas de los créditos para que caigan en el año pasado (2025).
-- Esto lo hago para asegurar que el bloque tenga datos reales que procesar.
UPDATE CREDITO_CLIENTE SET FECHA_SOLIC_CRED = TO_DATE('15/06/2025', 'DD/MM/YYYY');
COMMIT;

SET SERVEROUTPUT ON;

-- Variables para manejar los datos de entrada de forma ordenada.
VARIABLE b_rut_cli NUMBER;
VARIABLE b_p_norm NUMBER;
VARIABLE b_e1 NUMBER;
VARIABLE b_e2 NUMBER;
VARIABLE b_e3 NUMBER;
VARIABLE b_t1 NUMBER;
VARIABLE b_t2 NUMBER;

-- Configuro los valores para los cálculos de pesos y los topes de los tramos.
-- =============================================================================
-- DATOS PARA EJECUCIÓN CASO 1 (Asignar en variable :b_rut_cli)
-- Se entregan estos RUTs ya que coinciden con el script de poblado:
-- 1. 16947140 (PAMELA GATICA)
-- 2. 22176844 (SILVANA DUARTE)
-- 3. 16044255 (STEPHANIE DIAZ)
-- 4. 22558061 (AMANDA LIZANA)
-- 5. 16439752 (LUIS ALVAREZ)
-- =============================================================================
EXEC :b_rut_cli := 16947140;    -- Cambiar este RUT para cada caso de prueba
EXEC :b_p_norm := 1200;
EXEC :b_e1 := 100;
EXEC :b_e2 := 300;
EXEC :b_e3 := 550;
EXEC :b_t1 := 1000000;
EXEC :b_t2 := 3000000;

-- BLOQUE PARA EL CASO 1: LÓGICA TODOSUMA
SET SERVEROUTPUT ON;
DECLARE
    v_nro_cli          NUMBER;
    v_run_fmt          VARCHAR2(20);
    v_nombre_cli       VARCHAR2(200);
    v_tipo_cli_str     VARCHAR2(100);
    v_monto_anual_sol  NUMBER;
    v_total_pesos      NUMBER := 0;
    v_base_calculo     NUMBER := 100000;
BEGIN
    /* Saco los datos del cliente y el tipo de trabajador que es */
    -- Cruzo la tabla cliente con tipo_cliente para tener el nombre completo del tramo.
    SELECT c.NRO_CLIENTE, 
           c.NUMRUN || '-' || c.DVRUN,
           c.PNOMBRE || ' ' || NVL(c.SNOMBRE, '') || ' ' || c.APPATERNO || ' ' || c.APMATERNO,
           tc.NOMBRE_TIPO_CLIENTE
    INTO v_nro_cli, v_run_fmt, v_nombre_cli, v_tipo_cli_str
    FROM CLIENTE c
    JOIN TIPO_CLIENTE tc ON c.COD_TIPO_CLIENTE = tc.COD_TIPO_CLIENTE
    WHERE c.NUMRUN = :b_rut_cli;

    /* Busco cuánto pidió el cliente en créditos durante todo el año anterior */
    -- Uso EXTRACT para que el año sea relativo a la fecha de hoy menos uno.
    SELECT NVL(SUM(MONTO_SOLICITADO), 0)
    INTO v_monto_anual_sol
    FROM CREDITO_CLIENTE
    WHERE NRO_CLIENTE = v_nro_cli
      AND EXTRACT(YEAR FROM FECHA_SOLIC_CRED) = EXTRACT(YEAR FROM SYSDATE) - 1;

    /* Calculo los pesos que le corresponden */
    -- La base son 1.200 pesos por cada 100 lucas solicitadas.
    v_total_pesos := TRUNC(v_monto_anual_sol / v_base_calculo) * :b_p_norm;

    /* Si es independiente, reviso si le toca un extra adicional */
    -- Dependiendo de cuánto pidió el año pasado, le sumo el valor que corresponde al tramo.
    IF v_tipo_cli_str = 'Trabajadores independientes' AND v_monto_anual_sol > 0 THEN
        IF v_monto_anual_sol < :b_t1 THEN
            v_total_pesos := v_total_pesos + (TRUNC(v_monto_anual_sol / v_base_calculo) * :b_e1);
        ELSIF v_monto_anual_sol <= :b_t2 THEN
            v_total_pesos := v_total_pesos + (TRUNC(v_monto_anual_sol / v_base_calculo) * :b_e2);
        ELSE
            v_total_pesos := v_total_pesos + (TRUNC(v_monto_anual_sol / v_base_calculo) * :b_e3);
        END IF;
    END IF;

    /* Inserto los resultados en la tabla de destino */
    -- Primero borro al cliente si ya estaba para evitar que el script me tire error.
    DELETE FROM CLIENTE_TODOSUMA WHERE NRO_CLIENTE = v_nro_cli;

    INSERT INTO CLIENTE_TODOSUMA (
        NRO_CLIENTE, RUN_CLIENTE, NOMBRE_CLIENTE, 
        TIPO_CLIENTE, MONTO_SOLIC_CREDITOS, MONTO_PESOS_TODOSUMA
    )
    VALUES (
        v_nro_cli, v_run_fmt, v_nombre_cli, 
        v_tipo_cli_str, v_monto_anual_sol, v_total_pesos
    );

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Resultado: ' || v_nombre_cli || ' procesado con ' || v_total_pesos || ' pesos.');

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Aviso: No encontré información para el RUT ' || :b_rut_cli);
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error en el proceso: ' || SQLERRM);
END;
/

-- BLOQUE PARA EL CASO 2: POSTERGACIÓN DE CUOTAS
-- =========================================================================
    -- DATOS PARA EJECUCIÓN CASO 2 (Asignar en las variables de abajo)
    -- Estos tríos de datos (RUT, Solicitud, Cuotas) han sido validados:
    -- A) RUT: 12547896 | Solicitud: 2001 | Cuotas: 2
    -- B) RUT: 16947140 | Solicitud: 3004 | Cuotas: 1
    -- C) RUT: 12547896 | Solicitud: 2004 | Cuotas: 1
    -- =========================================================================
DECLARE
    -- Datos para hacer las pruebas con los clientes solicitados.
    v_rut_cli       NUMBER := 16947140; 
    v_solic_proc    NUMBER := 3004;     
    v_cant_post     NUMBER := 1;        

    v_id_cli        NUMBER;
    v_tipo_cred     NUMBER;
    v_v_cuota       NUMBER;
    v_n_cuota_max   NUMBER;
    v_f_venc_max    DATE;
    v_int           NUMBER := 0;
    v_count_cred    NUMBER;
BEGIN
    -- 1. Identifico al cliente y qué tipo de crédito tiene actualmente.
    SELECT NRO_CLIENTE, COD_CREDITO
    INTO v_id_cli, v_tipo_cred
    FROM CREDITO_CLIENTE 
    WHERE NRO_SOLIC_CREDITO = v_solic_proc;

    -- 2. Miro los datos de su última cuota para saber de dónde partir las nuevas.
    SELECT VALOR_CUOTA, NRO_CUOTA, FECHA_VENC_CUOTA
    INTO v_v_cuota, v_n_cuota_max, v_f_venc_max
    FROM CUOTA_CREDITO_CLIENTE
    WHERE NRO_SOLIC_CREDITO = v_solic_proc
    AND NRO_CUOTA = (SELECT MAX(NRO_CUOTA) FROM CUOTA_CREDITO_CLIENTE WHERE NRO_SOLIC_CREDITO = v_solic_proc);

    -- 3. Asigno el porcentaje de interés extra según el tipo de préstamo.
    IF v_tipo_cred = 1 THEN 
        IF v_cant_post = 1 THEN v_int := 0; ELSE v_int := 0.005; END IF;
    ELSIF v_tipo_cred = 2 THEN v_int := 0.01;
    ELSIF v_tipo_cred = 3 THEN v_int := 0.02;
    END IF;

    -- 4. Genero las cuotas nuevas sumando los meses correspondientes a la fecha.
    FOR i IN 1..v_cant_post LOOP
        INSERT INTO CUOTA_CREDITO_CLIENTE 
            (NRO_SOLIC_CREDITO, NRO_CUOTA, FECHA_VENC_CUOTA, VALOR_CUOTA)
        VALUES 
            (v_solic_proc, v_n_cuota_max + i, ADD_MONTHS(v_f_venc_max, i), ROUND(v_v_cuota * (1 + v_int)));
    END LOOP;

    -- 5. Chequeo si el cliente tiene más de un crédito del año pasado para condonar.
    SELECT COUNT(*) INTO v_count_cred FROM CREDITO_CLIENTE 
    WHERE NRO_CLIENTE = v_id_cli AND EXTRACT(YEAR FROM FECHA_SOLIC_CRED) = 2025;

    -- Si cumple la condición, le marco la cuota original como pagada.
    IF v_count_cred > 1 THEN
        UPDATE CUOTA_CREDITO_CLIENTE 
        SET FECHA_PAGO_CUOTA = FECHA_VENC_CUOTA, MONTO_PAGADO = VALOR_CUOTA
        WHERE NRO_SOLIC_CREDITO = v_solic_proc AND NRO_CUOTA = v_n_cuota_max;
        DBMS_OUTPUT.PUT_LINE('Beneficio de condonación aplicado a la cuota ' || v_n_cuota_max);
    END IF;

    DBMS_OUTPUT.PUT_LINE('PROCESO OK: Solicitud ' || v_solic_proc || ' finalizada.');
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE('Se produjo un error: ' || SQLERRM);
END;
/

-- Consultas rápidas para ver cómo quedaron las tablas después de correr el script.
SELECT * FROM CLIENTE_TODOSUMA;
SELECT * FROM CUOTA_CREDITO_CLIENTE WHERE NRO_SOLIC_CREDITO IN (2001, 3004, 2004);