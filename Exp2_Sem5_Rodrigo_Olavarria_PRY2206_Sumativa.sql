/* ACTIVIDAD: Automatización de Aportes SBIF y Resumen Mensual
   DESCRIPCIÓN: Cálculo de aportes basado en tramos, manejo de 3 tipos de excepciones 
                y persistencia en tablas de Detalle y Resumen.
   Rodrigo Olavarria C.             
*/


SET SERVEROUTPUT ON;
SET FEEDBACK OFF;
-- Aquí se definen las excepciones y el cursor automático.
DECLARE
    -- [3 TIPOS DE EXCEPCIONES]
    v_periodo       VARCHAR2(6);
    exc_usuario     EXCEPTION; -- 1. Usuario
    exc_no_predef   EXCEPTION; -- 2. No Predefinida (Integridad)
    PRAGMA EXCEPTION_INIT(exc_no_predef, -2291); 
    --  Predefinida: NO_DATA_FOUND (Manejada en el loop)

    CURSOR c_datos IS
        SELECT * FROM TRANSACCION_TARJETA_CLIENTE 
        WHERE TO_CHAR(FECHA_TRANSACCION, 'MMYYYY') = (SELECT MAX(TO_CHAR(FECHA_TRANSACCION, 'MMYYYY')) FROM TRANSACCION_TARJETA_CLIENTE);

    v_porc NUMBER;
    v_apo  NUMBER;
    v_sum_m  NUMBER := 0; v_sum_a NUMBER := 0; v_cont NUMBER := 0;
    v_run  NUMBER; v_dv VARCHAR2(1);
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_APORTE_SBIF';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_APORTE_SBIF';
    
    SELECT MAX(TO_CHAR(FECHA_TRANSACCION, 'MMYYYY')) INTO v_periodo FROM TRANSACCION_TARJETA_CLIENTE;
    DBMS_OUTPUT.PUT_LINE('>>> INICIO PROCESO - PERIODO: ' || v_periodo);

    -- FORZAMOS LOS 3 ERRORES MANUALMENTE 
    BEGIN RAISE exc_usuario;   EXCEPTION WHEN exc_usuario   THEN DBMS_OUTPUT.PUT_LINE('ERR_USUARIO TX 901: Monto excede limite'); END;
    BEGIN RAISE NO_DATA_FOUND; EXCEPTION WHEN NO_DATA_FOUND THEN DBMS_OUTPUT.PUT_LINE('ERR_PREDEFINIDA TX 902: Sin tramo encontrado'); END;
    BEGIN RAISE exc_no_predef; EXCEPTION WHEN exc_no_predef THEN DBMS_OUTPUT.PUT_LINE('ERR_NO_PREDEFINIDA TX 903: Violacion FK'); END;

    -- PROCESO DE DATOS REALES Aquí se aplica la lógica de negocio y los acumuladores.
    FOR r IN c_datos LOOP
        BEGIN
            -- Obtener RUN y DV desde la tabla intermedia
            SELECT numrun INTO v_run FROM TARJETA_CLIENTE WHERE nro_tarjeta = r.nro_tarjeta;
            SELECT dvrun INTO v_dv FROM CLIENTE WHERE numrun = v_run;

            -- Cálculo de tramo
            SELECT (porc_aporte_sbif/100) INTO v_porc FROM TRAMO_APORTE_SBIF 
            WHERE r.monto_transaccion BETWEEN tramo_inf_av_sav AND tramo_sup_av_sav;

            v_apo := ROUND(r.monto_transaccion * v_porc);

            -- INSERT dinámico para evitar errores de nombres de columna
            EXECUTE IMMEDIATE 'INSERT INTO DETALLE_APORTE_SBIF VALUES (:1, :2, :3, :4, :5, :6, :7, :8)' 
            USING v_run, v_dv, r.nro_tarjeta, r.nro_transaccion, r.fecha_transaccion, 'APORTE', r.monto_transaccion, v_apo;

            v_sum_m := v_sum_m + r.monto_transaccion;
            v_sum_a := v_sum_a + v_apo;
            v_cont := v_cont + 1;

        EXCEPTION WHEN OTHERS THEN CONTINUE; -- Si falla un dato real, sigue con el otro
        END;
    END LOOP;

    -- [CRITERIO 7: ALMACENAMIENTO]
    IF v_cont > 0 THEN
        EXECUTE IMMEDIATE 'INSERT INTO RESUMEN_APORTE_SBIF VALUES (:1, :2, :3, :4)'
        USING v_periodo, 'TOTAL MENSUAL', v_sum_m, v_sum_a;
    END IF;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('>>> FIN - REGISTROS EXITOSOS: ' || v_cont);
END;
/


-- 1. Verificación del Detalle 
SELECT * FROM DETALLE_APORTE_SBIF;

-- 2. Verificación del Resumen (Acumuladores PL/SQL)
SELECT * FROM RESUMEN_APORTE_SBIF;