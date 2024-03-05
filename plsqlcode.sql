DECLARE
    -- Declare the cursor for selecting data from source_trades
    CURSOR c_source_data IS
        SELECT * FROM source_trades;

    -- Type based on the structure of source_trades
    TYPE t_table_data IS TABLE OF source_trades%ROWTYPE;
    -- Variable for holding fetched data
    v_data t_table_data;

    -- Limit for bulk collect
    c_limit CONSTANT PLS_INTEGER := 1000;

      v_error_message VARCHAR2(4000) := SQLERRM;
    v_timestamp TIMESTAMP := SYSTIMESTAMP;
BEGIN
    -- Open the cursor
    OPEN c_source_data;

    LOOP
        -- Fetch data into the collection
        FETCH c_source_data BULK COLLECT INTO v_data LIMIT c_limit;

        EXIT WHEN v_data.COUNT = 0;

        BEGIN
            -- Example FORALL statement assuming transformation/aggregation logic is handled elsewhere
            FORALL i IN 1..v_data.COUNT
                INSERT INTO analyzed_trades (analysis_id, source_trade_id, daily_volume, average_price, analysis_date)
                VALUES (v_data(i).trade_id, v_data(i).security_id, v_data(i).volume, v_data(i).price, v_data(i).trade_date);
        EXCEPTION
            WHEN OTHERS THEN
                -- Error handling: attempt individual inserts for more granularity in error capture
                FOR i IN 1..v_data.COUNT LOOP
                    BEGIN
                        INSERT INTO analyzed_trades (analysis_id, source_trade_id, daily_volume, average_price, analysis_date)
                        VALUES (v_data(i).trade_id, v_data(i).security_id, v_data(i).volume, v_data(i).price, v_data(i).trade_date);
                    EXCEPTION
                        WHEN OTHERS THEN
                            INSERT INTO error_logs (error_message, timestamp)
                            VALUES (v_error_message, v_timestamp);
                    END;
                END LOOP;
        END;

        -- Check if fewer rows than limit were fetched, indicating end of data
        EXIT WHEN v_data.COUNT < c_limit;
    END LOOP;

    -- Close the cursor
    CLOSE c_source_data;
EXCEPTION
    WHEN OTHERS THEN
        -- Log unhandled exceptions
        INSERT INTO error_logs (error_message, timestamp)
        VALUES (v_error_message, SYSTIMESTAMP);
        RAISE; -- Re-raise exception after logging
END;
