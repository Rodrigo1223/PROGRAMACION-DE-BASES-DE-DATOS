SET SERVEROUTPUT ON;

-- =============================================================================
-- CASO 1: "CÍRCULO ALL THE BEST"
-- =============================================================================

-- Variables BIND para el ingreso paramétrico de tramos 
VARIABLE b_tramo1 NUMBER;
VARIABLE b_tramo2 NUMBER;
VARIABLE b_tramo3 NUMBER;
EXEC :b_tramo1 := 500000;
EXEC :b_tramo2 := 700001;
EXEC :b_tramo3 := 900001;

DECLARE
    --VARRAY para puntos normales y extras 
    TYPE t_array_puntos IS VARRAY(4) OF NUMBER;
    va_puntos t_array_puntos := t_array_puntos(250, 300, 550, 700);

    --Registro PL/SQL para datos del cliente 
    TYPE t_reg_cliente IS RECORD (
        run      CLIENTE.NUMRUN%TYPE,
        dv       CLIENTE.DVRUN%TYPE,
        tipo_cli CLIENTE.COD_TIPO_CLIENTE%TYPE
    );
    reg_cli t_reg_cliente;

    --Variable de Cursor (REF CURSOR) para clientes 
    TYPE t_cur_var IS REF CURSOR;
    cv_clientes t_cur_var;

    --Cursor Explícito con Parámetro para transacciones anuales 
    CURSOR c_transacciones(p_run NUMBER, p_anio NUMBER) IS
        SELECT tar.NRO_TARJETA, tra.NRO_TRANSACCION, tra.FECHA_TRANSACCION, 
               tp.NOMBRE_TPTRAN_TARJETA, tra.MONTO_TRANSACCION, tra.COD_TPTRAN_TARJETA
        FROM TARJETA_CLIENTE tar
        JOIN TRANSACCION_TARJETA_CLIENTE tra ON tar.NRO_TARJETA = tra.NRO_TARJETA
        JOIN TIPO_TRANSACCION_TARJETA tp ON tra.COD_TPTRAN_TARJETA = tp.COD_TPTRAN_TARJETA
        WHERE tar.NUMRUN = p_run 
          AND EXTRACT(YEAR FROM tra.FECHA_TRANSACCION) = p_anio;

    --Obtención dinámica del año anterior 
    v_anio_anterior NUMBER := EXTRACT(YEAR FROM SYSDATE) - 1; 
    v_puntos_base   NUMBER;
    v_puntos_extra  NUMBER;
    v_total_puntos  NUMBER;
    v_monto_anual   NUMBER;

BEGIN
    --Truncar tablas en tiempo de ejecución usando SQL Dinámico 
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_PUNTOS_TARJETA_CATB';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_PUNTOS_TARJETA_CATB';

    --Procesamiento simultáneo de clientes y transacciones
    OPEN cv_clientes FOR SELECT NUMRUN, DVRUN, COD_TIPO_CLIENTE FROM CLIENTE;
    LOOP
        FETCH cv_clientes INTO reg_cli;
        EXIT WHEN cv_clientes%NOTFOUND;

        -- Cálculo del monto total anual para evaluar puntos extras
        SELECT NVL(SUM(tra.MONTO_TRANSACCION), 0) INTO v_monto_anual
        FROM TARJETA_CLIENTE tar
        JOIN TRANSACCION_TARJETA_CLIENTE tra ON tar.NRO_TARJETA = tra.NRO_TARJETA
        WHERE tar.NUMRUN = reg_cli.run AND EXTRACT(YEAR FROM tra.FECHA_TRANSACCION) = v_anio_anterior;

        FOR r_tra IN c_transacciones(reg_cli.run, v_anio_anterior) LOOP
            --Cálculo de puntos en PL/SQL con estructura condicional 
            v_puntos_base := (r_tra.MONTO_TRANSACCION / 100000) * va_puntos(1);
            v_puntos_extra := 0;

            -- Lógica para Dueñas de Casa (3) y Pensionados (4)
            IF reg_cli.tipo_cli IN (3, 4) THEN
                IF v_monto_anual BETWEEN :b_tramo1 AND 700000 THEN
                    v_puntos_extra := (r_tra.MONTO_TRANSACCION / 100000) * va_puntos(2);
                ELSIF v_monto_anual BETWEEN :b_tramo2 AND 900000 THEN
                    v_puntos_extra := (r_tra.MONTO_TRANSACCION / 100000) * va_puntos(3);
                ELSIF v_monto_anual >= :b_tramo3 THEN
                    v_puntos_extra := (r_tra.MONTO_TRANSACCION / 100000) * va_puntos(4);
                END IF;
            END IF;

            v_total_puntos := v_puntos_base + v_puntos_extra;

            -- Inserción en tabla Detalle 
            INSERT INTO DETALLE_PUNTOS_TARJETA_CATB 
            VALUES (reg_cli.run, reg_cli.dv, r_tra.NRO_TARJETA, r_tra.NRO_TRANSACCION, 
                    r_tra.FECHA_TRANSACCION, r_tra.NOMBRE_TPTRAN_TARJETA, 
                    r_tra.MONTO_TRANSACCION, ROUND(v_total_puntos));
        END LOOP;
    END LOOP;
    CLOSE cv_clientes;

    --Llenado de Resumen agrupado y ordenado por mes 
    --Se usa PUNTOS_ALLTHEBEST según la definición de tu tabla
    INSERT INTO RESUMEN_PUNTOS_TARJETA_CATB
    SELECT 
        TO_CHAR(FECHA_TRANSACCION, 'YYYYMM'),
        SUM(CASE WHEN TIPO_TRANSACCION NOT LIKE '%Avance%' THEN MONTO_TRANSACCION ELSE 0 END),
        SUM(CASE WHEN TIPO_TRANSACCION NOT LIKE '%Avance%' THEN PUNTOS_ALLTHEBEST ELSE 0 END),
        SUM(CASE WHEN TIPO_TRANSACCION = 'Avance en Efectivo' THEN MONTO_TRANSACCION ELSE 0 END),
        SUM(CASE WHEN TIPO_TRANSACCION = 'Avance en Efectivo' THEN PUNTOS_ALLTHEBEST ELSE 0 END),
        SUM(CASE WHEN TIPO_TRANSACCION = 'Súper Avance en Efectivo' THEN MONTO_TRANSACCION ELSE 0 END),
        SUM(CASE WHEN TIPO_TRANSACCION = 'Súper Avance en Efectivo' THEN PUNTOS_ALLTHEBEST ELSE 0 END)
    FROM DETALLE_PUNTOS_TARJETA_CATB
    GROUP BY TO_CHAR(FECHA_TRANSACCION, 'YYYYMM')
    ORDER BY 1 ASC;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('CASO 1 FINALIZADO.');
END;
/

-- CONSULTAS DE VERIFICACIÓN CASO 1
SELECT * FROM DETALLE_PUNTOS_TARJETA_CATB ORDER BY FECHA_TRANSACCION, NUMRUN;
SELECT * FROM RESUMEN_PUNTOS_TARJETA_CATB ORDER BY MES_ANNO;


-- =============================================================================
-- CASO 2: LEY DE APORTES SBIF
-- =============================================================================

DECLARE
    CURSOR c_clientes IS SELECT numrun, dvrun FROM cliente;

    CURSOR c_transacciones(p_run NUMBER, p_anio NUMBER) IS
        SELECT t.nro_tarjeta, t.nro_transaccion, t.fecha_transaccion, 
               tt.nombre_tptran_tarjeta, t.monto_total_transaccion
        FROM transaccion_tarjeta_cliente t
        JOIN tarjeta_cliente tc ON t.nro_tarjeta = tc.nro_tarjeta
        JOIN tipo_transaccion_tarjeta tt ON t.cod_tptran_tarjeta = tt.cod_tptran_tarjeta
        WHERE tc.numrun = p_run
          AND (UPPER(tt.nombre_tptran_tarjeta) LIKE '%AVANCE%')
          AND EXTRACT(YEAR FROM t.fecha_transaccion) = p_anio;

    v_anio_proceso  NUMBER := EXTRACT(YEAR FROM SYSDATE) - 2; -- Ajustado para encontrar tus datos 2024
    v_porc_aporte   NUMBER;
    v_monto_aporte  NUMBER;

BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_APORTE_SBIF';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_APORTE_SBIF';

    FOR r_cli IN c_clientes LOOP
        FOR r_tra IN c_transacciones(r_cli.numrun, v_anio_proceso) LOOP
            BEGIN
                SELECT (porc_aporte_sbif / 100) INTO v_porc_aporte
                FROM tramo_aporte_sbif
                WHERE r_tra.monto_total_transaccion BETWEEN tramo_inf_av_sav AND tramo_sup_av_sav;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN v_porc_aporte := 0;
            END;

            v_monto_aporte := r_tra.monto_total_transaccion * v_porc_aporte;

            INSERT INTO detalle_aporte_sbif 
            VALUES (r_cli.numrun, r_cli.dvrun, r_tra.nro_tarjeta, r_tra.nro_transaccion, 
                    r_tra.fecha_transaccion, r_tra.nombre_tptran_tarjeta, 
                    r_tra.monto_total_transaccion, ROUND(v_monto_aporte));
        END LOOP;
    END LOOP;

    INSERT INTO resumen_aporte_sbif
    SELECT TO_CHAR(fecha_transaccion, 'YYYYMM'), tipo_transaccion, 
           SUM(monto_transaccion), SUM(aporte_sbif)
    FROM detalle_aporte_sbif
    GROUP BY TO_CHAR(fecha_transaccion, 'YYYYMM'), tipo_transaccion
    ORDER BY 1 ASC;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('CASO 2 FINALIZADO.');
END;
/

-- CONSULTAS DE VERIFICACIÓN CASO 2
SELECT * FROM DETALLE_APORTE_SBIF ORDER BY FECHA_TRANSACCION ASC;
SELECT * FROM RESUMEN_APORTE_SBIF ORDER BY MES_ANNO ASC;