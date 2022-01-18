-- ## Ejecuta la misma consulta por cada esquema con los mismos patrones numéricos
DECLARE
  NO_EXISTE EXCEPTION;
  l_sql_stmt VARCHAR(32767);
BEGIN
	FOR t IN (
        -- enlista todos los esquemas en la base de datos
		SELECT username AS schema_name 
		FROM sys.all_users 
        -- se define patrón numérico para limitar los esquemas según criterios de necesidad
		WHERE SUBSTR(username,11,1) IN ('1','2','3','4','5','6','7','8','9','0')
	) 
    -- inicio iteración 
    LOOP 
        -- Agregación de suma por periodo para todo el segmento de datos por entidad 
        BEGIN
        EXECUTE IMMEDIATE
            'SELECT periodo, SUM(a.saldo+a.deuda+a.intereses) deuda_real 
             FROM '||t.schema_name||'.table_name a
             GROUP BY periodo' INTO l_sql_stmt;
        DBMS_OUTPUT.PUT_LINE(t.schema_name || ' ==> ' || l_sql_stmt );
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN
                    RAISE;
                END IF;
        END;
    END LOOP;
END;