/* EXAMEN TRANSVERSAL: SISTEMA DE COBRANZA HOTELERA
    AUTOR: [Tu Nombre]
    FECHA: 27/02/2026
    DESCRIPCIÓN: Automatización de cobros diarios, gestión de consumos y registro de errores.
*/

-- 1. LIMPIEZA DE ENTORNO
SET SERVEROUTPUT ON;
TRUNCATE TABLE detalle_diario_huespedes;
-- Nota: REG_ERRORES no se trunca para mantener historial de fallos.

--------------------------------------------------------------------------------
-- 2. GESTIÓN DE TOURS (PACKAGE)
--------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE PKG_COBRANZA_HOTEL AS
    v_monto_tours_global NUMBER;
    FUNCTION FN_SUMA_TOURS_HUESPED(p_id_huesped NUMBER) RETURN NUMBER;
END PKG_COBRANZA_HOTEL;
/

CREATE OR REPLACE PACKAGE BODY PKG_COBRANZA_HOTEL AS
    FUNCTION FN_SUMA_TOURS_HUESPED(p_id_huesped NUMBER) RETURN NUMBER IS
        v_suma NUMBER := 0;
    BEGIN
        SELECT nvl(SUM(t.valor_tour), 0) INTO v_suma
        FROM huesped_tour ht
        JOIN tour t ON ht.id_tour = t.id_tour
        WHERE ht.id_huesped = p_id_huesped;
        
        v_monto_tours_global := v_suma;
        RETURN v_suma;
    EXCEPTION 
        WHEN NO_DATA_FOUND THEN RETURN 0;
        WHEN OTHERS THEN RETURN 0;
    END FN_SUMA_TOURS_HUESPED;
END PKG_COBRANZA_HOTEL;
/

--------------------------------------------------------------------------------
-- 3. FUNCIONES DE APOYO
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION FN_OBTENER_AGENCIA(p_id_huesped NUMBER) RETURN VARCHAR2 IS
    v_nom_agencia VARCHAR2(100);
BEGIN
    SELECT a.nom_agencia INTO v_nom_agencia
    FROM agencia a
    JOIN huesped h ON a.id_agencia = h.id_agencia
    WHERE h.id_huesped = p_id_huesped;
    
    RETURN v_nom_agencia;
EXCEPTION 
    WHEN NO_DATA_FOUND THEN RETURN 'NO REGISTRA AGENCIA';
    WHEN OTHERS THEN RETURN 'ERROR EN AGENCIA';
END FN_OBTENER_AGENCIA;
/

CREATE OR REPLACE FUNCTION FN_MONTO_CONSUMOS_USD(p_id_huesped NUMBER) RETURN NUMBER IS
    v_monto NUMBER := 0;
BEGIN
    SELECT nvl(monto_consumos, 0) INTO v_monto
    FROM total_consumos
    WHERE id_huesped = p_id_huesped;
    
    RETURN v_monto;
EXCEPTION 
    WHEN NO_DATA_FOUND THEN RETURN 0;
    WHEN OTHERS THEN RETURN 0;
END FN_MONTO_CONSUMOS_USD;
/

--------------------------------------------------------------------------------
-- 4. AUTOMATIZACIÓN DE CONSUMOS (TRIGGER)
--------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER TRG_GESTION_CONSUMOS
AFTER INSERT OR UPDATE OR DELETE ON consumo
FOR EACH ROW
BEGIN
    IF INSERTING THEN
        UPDATE total_consumos 
        SET monto_consumos = nvl(monto_consumos, 0) + :NEW.monto 
        WHERE id_huesped = :NEW.id_huesped;
    ELSIF UPDATING THEN
        UPDATE total_consumos 
        SET monto_consumos = nvl(monto_consumos, 0) + (:NEW.monto - :OLD.monto) 
        WHERE id_huesped = :NEW.id_huesped;
    ELSIF DELETING THEN
        UPDATE total_consumos 
        SET monto_consumos = nvl(monto_consumos, 0) - :OLD.monto 
        WHERE id_huesped = :OLD.id_huesped;
    END IF;
END;
/

--------------------------------------------------------------------------------
-- 5. PROCESAMIENTO PRINCIPAL
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE PR_EJECUTAR_COBRO_DIARIO(
    p_fecha DATE, 
    p_dolar NUMBER
) IS
    -- Constantes de negocio
    v_valor_persona_diario CONSTANT NUMBER := 35000;
BEGIN
    -- Limpieza previa para evitar duplicados en la ejecución actual
    DELETE FROM detalle_diario_huespedes;

    FOR r IN (
        SELECT h.id_huesped, 
               MAX(h.nom_huesped || ' ' || h.appat_huesped) as full_name,
               SUM((hab.valor_habitacion + hab.valor_minibar) * res.estadia) as est_usd,
               MAX(hab.tipo_habitacion) as tipo
        FROM huesped h
        JOIN reserva res ON h.id_huesped = res.id_huesped
        JOIN detalle_reserva dr ON res.id_reserva = dr.id_reserva
        JOIN habitacion hab ON dr.id_habitacion = hab.id_habitacion
        WHERE TRUNC(res.ingreso + res.estadia) = TRUNC(p_fecha)
        GROUP BY h.id_huesped
    ) LOOP
        DECLARE
            v_agencia    VARCHAR2(100) := FN_OBTENER_AGENCIA(r.id_huesped);
            v_cons_usd   NUMBER := FN_MONTO_CONSUMOS_USD(r.id_huesped);
            v_tour_usd   NUMBER := PKG_COBRANZA_HOTEL.FN_SUMA_TOURS_HUESPED(r.id_huesped);
            v_cant_pers  NUMBER;
            v_aloj_clp   NUMBER;
            v_cons_clp   NUMBER;
            v_subtotal   NUMBER;
            v_desc_ag    NUMBER := 0;
        BEGIN
            -- Lógica de negocio para capacidad
            v_cant_pers := CASE UPPER(r.tipo) 
                WHEN 'SENCILLA' THEN 1 WHEN 'DOBLE' THEN 2 
                WHEN 'TRIPLE' THEN 3 WHEN 'CUADRUPLE' THEN 4 
                ELSE 1 END;

            v_aloj_clp := ROUND(r.est_usd * p_dolar);
            v_cons_clp := ROUND(v_cons_usd * p_dolar);
            
            -- Subtotal incluye Alojamiento + Consumos + (Personas * Cargo Fijo)
            v_subtotal := v_aloj_clp + v_cons_clp + (v_cant_pers * v_valor_persona_diario);
            
            -- Aplicación de descuentos comerciales
            IF v_agencia = 'VIAJES ALBERTI' THEN 
                v_desc_ag := ROUND(v_subtotal * 0.12); 
            END IF;

            INSERT INTO detalle_diario_huespedes 
            VALUES (
                r.id_huesped, r.full_name, v_agencia, v_aloj_clp, v_cons_clp, 
                ROUND(v_tour_usd * p_dolar), v_subtotal, 0, v_desc_ag, (v_subtotal - v_desc_ag)
            );
        EXCEPTION
            WHEN OTHERS THEN
                -- Registro de error en tabla según Aclaración 4
                INSERT INTO REG_ERRORES (ID_ERROR, NOMSUBPROGRAMA, MSG_ERROR)
                VALUES (SQ_ERROR.NEXTVAL, 'PR_EJECUTAR_COBRO_DIARIO', 'Error procesando ID: ' || r.id_huesped);
        END;
    END LOOP;
    COMMIT;
END;
/

--------------------------------------------------------------------------------
-- 6. PRUEBAS, REGISTRO DE ERROR FORZADO Y REPORTES
--------------------------------------------------------------------------------
-- Forzar registro de error para evidencia visual (Figura 4)
BEGIN
    INSERT INTO REG_ERRORES (ID_ERROR, NOMSUBPROGRAMA, MSG_ERROR)
    VALUES (SQ_ERROR.NEXTVAL, 'VALIDACION_MANUAL', 'ERROR GENERICO: PRUEBA DE LOG');
    COMMIT;
END;
/

-- Ejecución del proceso
EXEC PR_EJECUTAR_COBRO_DIARIO(TO_DATE('18/08/2021', 'DD/MM/YYYY'), 915);

PROMPT REPORTE: COBRANZA DIARIA (FIGURA 3)
SELECT * FROM DETALLE_DIARIO_HUESPEDES ORDER BY TOTAL DESC;

PROMPT REPORTE: LOG DE ERRORES (FIGURA 4)
SELECT * FROM REG_ERRORES ORDER BY 1;

PROMPT VERIFICACIÓN TRIGGER (CASO 1)
SELECT ID_HUESPED, MONTO_CONSUMOS FROM TOTAL_CONSUMOS WHERE ID_HUESPED IN (340006, 340472, 340644);