/*
Copyright 2019 Dirk Strack, Strack Software Development

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
/*
-- deinstall
begin
    for c in (
        select JOB_NAME from USER_SCHEDULER_JOBS
        where JOB_NAME LIKE 'DBR_VIS%'
    ) loop 
        dbms_scheduler.drop_job (
            job_name => c.JOB_NAME,
            force => TRUE
        );
    end loop;
    commit;
end;
/
DROP VIEW CLOUD_VISITORS_V;
DROP TABLE CLOUD_VISITORS;
DROP TABLE CLOUD_VISITORS_EXCLUDED_IP_LIST;
DROP TABLE CLOUD_VISITORS_IP_BLACK_LIST;
DROP SEQUENCE CLOUD_VISITORS_SEQ;
DROP PACKAGE CLOUD_VISITORS_UTL;

*/

CREATE OR REPLACE VIEW CLOUD_VISITORS_V (
    LAST_LOGIN_DATE, LOGIN_CNT, APPLICATION_ID, PAGE_ID, APPLICATION_NAME, APEX_USER,
    IP_ADDRESS, AGENT, PAGE_NAME, REQUESTS, CNT, ELAPSED_TIME, DURATION_MINS
)
AS
with clients_q as (
    SELECT to_char(CAST((MAX(VIEW_TIMESTAMP) AT TIME ZONE 'Europe/Berlin') AS DATE)
                , 'YY.MM.DD HH24:MI.SS') LAST_LOGIN_DATE, 
        COUNT(*) LOGIN_CNT,
        APPLICATION_ID, IP_ADDRESS, APEX_SESSION_ID, AGENT
    FROM APEX_WORKSPACE_ACTIVITY_LOG
    WHERE IP_ADDRESS IS NOT NULL
    AND PAGE_ID != 0
    GROUP BY APPLICATION_ID, IP_ADDRESS, APEX_SESSION_ID, AGENT
    ORDER BY MAX(VIEW_TIMESTAMP) DESC
), pages_q as (
    select APPLICATION_ID, APPLICATION_NAME, APEX_USER, APEX_SESSION_ID, PAGE_ID, PAGE_NAME,
        LISTAGG(REQUEST_VALUE, ', ') WITHIN GROUP (ORDER BY REQUEST_VALUE) as REQUESTS,
        SUM(CNT) CNT, 
        MIN(DATE_FROM) DATE_FROM, 
        MAX(DATE_TO) DATE_TO, 
        SUM(ELAPSED_TIME) ELAPSED_TIME
    from (
        select APPLICATION_ID, APPLICATION_NAME, APEX_USER, APEX_SESSION_ID, PAGE_ID, PAGE_NAME, REQUEST_VALUE,
            COUNT(*) CNT, 
            CAST((MIN(VIEW_TIMESTAMP) AT TIME ZONE 'Europe/Berlin') AS DATE) DATE_FROM, 
            CAST((MAX(VIEW_TIMESTAMP) AT TIME ZONE 'Europe/Berlin') AS DATE) DATE_TO,
            SUM(ELAPSED_TIME) ELAPSED_TIME
        from APEX_WORKSPACE_ACTIVITY_LOG
        where PAGE_ID != 0
        group by APPLICATION_ID, APPLICATION_NAME, APEX_USER, APEX_SESSION_ID, PAGE_ID, PAGE_NAME, REQUEST_VALUE
    )
    group by APPLICATION_ID, APPLICATION_NAME, APEX_USER, APEX_SESSION_ID, PAGE_ID, PAGE_NAME
), page_totals_q as (
    select MAX(c.LAST_LOGIN_DATE) LAST_LOGIN_DATE, 
        sum(c.LOGIN_CNT) LOGIN_CNT, 
        c.APPLICATION_ID, 
        c.PAGE_ID, 
        c.APPLICATION_NAME,
        c.APEX_USER,
        c.IP_ADDRESS, c.AGENT,
        c.PAGE_NAME, 
        c.REQUESTS, 
        sum(c.CNT) CNT, 
        round(sum(c.ELAPSED_TIME), 2) ELAPSED_TIME, 
        round(sum(c.DATE_TO - c.DATE_FROM) * 24 * 60, 2)  DURATION_MINS
    from (
        select p.PAGE_ID, p.PAGE_NAME, p.REQUESTS, p.CNT, p.DATE_FROM, p.DATE_TO, p.ELAPSED_TIME,
                c.LAST_LOGIN_DATE, c.LOGIN_CNT, c.APPLICATION_ID, p.APPLICATION_NAME, p.APEX_USER,
                c.IP_ADDRESS, c.APEX_SESSION_ID, c.AGENT
        from clients_q c 
        join pages_q p on c.APPLICATION_ID = p.APPLICATION_ID and c.APEX_SESSION_ID = p.APEX_SESSION_ID
    ) c 
    group by c.APPLICATION_ID, c.APPLICATION_NAME, c.APEX_USER, c.IP_ADDRESS, c.AGENT, c.PAGE_ID, c.PAGE_NAME, c.REQUESTS
) ---------------------------------------------------------------------------------------
select c.LAST_LOGIN_DATE, c.LOGIN_CNT, 
    c.APPLICATION_ID, c.PAGE_ID, c.APPLICATION_NAME, c.APEX_USER, 
    c.IP_ADDRESS, c.AGENT,
    PAGE_NAME, REQUESTS, CNT, ELAPSED_TIME, DURATION_MINS
from page_totals_q c
where APPLICATION_ID NOT between 4000 and 4999
order by LAST_LOGIN_DATE desc, IP_ADDRESS, PAGE_ID
;




declare
    v_Count PLS_INTEGER;
begin
    SELECT COUNT(*) INTO v_Count
    from sys.user_tables t 
    where t.table_name = 'CLOUD_VISITORS'
    and not exists (
        select 1 from sys.user_tab_cols c where c.table_name = t.table_name and c.column_name = 'APEX_USER'  -- latest added column.
    );
    if v_Count = 1 then
        EXECUTE IMMEDIATE 'DROP TABLE CLOUD_VISITORS';
    end if; 

    SELECT COUNT(*) INTO v_count
    FROM USER_SEQUENCES WHERE SEQUENCE_NAME = 'CLOUD_VISITORS_SEQ';
    if v_count = 0 then 
        EXECUTE IMMEDIATE 'CREATE SEQUENCE CLOUD_VISITORS_SEQ START WITH 1 INCREMENT BY 1 NOCYCLE';
    end if;

    SELECT COUNT(*) INTO v_Count
    from sys.user_tables where table_name = 'CLOUD_VISITORS';
    if v_Count = 0 then
        EXECUTE IMMEDIATE q'[
    CREATE TABLE CLOUD_VISITORS (
        ID              NUMBER DEFAULT ON NULL CLOUD_VISITORS_SEQ.NEXTVAL NOT NULL,
        WEB_MODULE_ID   VARCHAR2(255) NOT NULL, 
        LAST_LOGIN_DATE VARCHAR2(20) NOT NULL, 
        LOGIN_CNT       NUMBER, 
        APPLICATION_ID  NUMBER NOT NULL, 
        PAGE_ID         NUMBER NOT NULL,
        APPLICATION_NAME VARCHAR2(255), 
        APEX_USER 		VARCHAR2(255),
        IP_ADDRESS      VARCHAR2(4000) NOT NULL, 
        IP_LOCATION     VARCHAR2(4000), 
        AGENT           VARCHAR2(4000),
        PAGE_NAME       VARCHAR2(4000),
        REQUESTS        VARCHAR2(4000),
        CNT             NUMBER,
        ELAPSED_TIME    VARCHAR2(20),
        DURATION_MINS   VARCHAR2(20),
        CONTINENTCODE   VARCHAR2(2),
        CONTINENTNAME   VARCHAR2(500),
        COUNTRYCODE     VARCHAR2(2),
        COUNTRYNAME     VARCHAR2(500),
        STATEPROV       VARCHAR2(500),
        CITY            VARCHAR2(500),
        CONSTRAINT CLOUD_VISITORS_PK PRIMARY KEY (ID) USING INDEX,
        CONSTRAINT CLOUD_VISITORS_UK UNIQUE (WEB_MODULE_ID, LAST_LOGIN_DATE, IP_ADDRESS, APPLICATION_ID, PAGE_ID)  USING INDEX COMPRESS 4
    )
        ]';
    end if; 

    SELECT COUNT(*) INTO v_Count
    from sys.user_tables where table_name = 'CLOUD_VISITORS_EXCLUDED_IP_LIST';
    if v_Count = 0 then
        EXECUTE IMMEDIATE q'[
    CREATE TABLE CLOUD_VISITORS_EXCLUDED_IP_LIST (
        ID              NUMBER DEFAULT ON NULL CLOUD_VISITORS_SEQ.NEXTVAL NOT NULL,
        IP_ADDRESS      VARCHAR2(512) NOT NULL, 
        CREATED_AT TIMESTAMP (6) WITH LOCAL TIME ZONE DEFAULT LOCALTIMESTAMP NOT NULL ENABLE, 
        CREATED_BY VARCHAR2(32 CHAR) DEFAULT NVL(SYS_CONTEXT('APEX$SESSION','APP_USER'), SYS_CONTEXT('USERENV','SESSION_USER')) NOT NULL ENABLE, 
        CONSTRAINT CLOUD_VISITORS_EXCLUDED_IP_LIST_PK PRIMARY KEY (ID),
        CONSTRAINT CLOUD_VISITORS_EXCLUDED_IP_LIST_UN UNIQUE (IP_ADDRESS)
    )
        ]';
    end if; 

    SELECT COUNT(*) INTO v_Count
    from sys.user_objects where object_name = 'CLOUD_VISITORS_UTL' and object_type = 'PACKAGE';
    if v_Count > 0 then
        EXECUTE IMMEDIATE 'DROP PACKAGE CLOUD_VISITORS_UTL';
    end if; 
end;
/


CREATE OR REPLACE PACKAGE CLOUD_VISITORS_UTL 
AUTHID CURRENT_USER
IS
	g_debug BOOLEAN := TRUE;
    type appvisitor_row_t IS RECORD (
        LAST_LOGIN_DATE VARCHAR2(20),
        LOGIN_CNT       NUMBER,
        APPLICATION_ID  NUMBER,
        PAGE_ID         NUMBER, 
        APPLICATION_NAME VARCHAR2(255), 
        APEX_USER		VARCHAR2(255), 
        IP_ADDRESS      VARCHAR2(4000),
        AGENT           VARCHAR2(4000),
        PAGE_NAME       VARCHAR2(4000),
        REQUESTS        VARCHAR2(4000),
        CNT             NUMBER,
        ELAPSED_TIME    VARCHAR2(20),
        DURATION_MINS   VARCHAR2(20)
    );
    type appvisitor_table_t is table of appvisitor_row_t;
    
    type db_ip_geoloc_free_row_t IS RECORD (
        IP_ADDRESS      VARCHAR2(500),
        CONTINENTCODE   VARCHAR2(2),
        CONTINENTNAME   VARCHAR2(500),
        COUNTRYCODE     VARCHAR2(2),
        COUNTRYNAME     VARCHAR2(500),
        STATEPROV       VARCHAR2(500),
        CITY            VARCHAR2(500),
        ERROR           VARCHAR2(500),
        ERRORCODE       VARCHAR2(500)
    );
    type db_ip_geoloc_free_table_t IS table of db_ip_geoloc_free_row_t;
    
    function Pipe_Db_Ip_Geoloc_Rest (
        p_ipAddress VARCHAR2 DEFAULT 'self',
        p_geoloc_module_static_id VARCHAR2 DEFAULT 'IP_Geolocation',
        p_API_Key VARCHAR2 DEFAULT 'free'
    ) return db_ip_geoloc_free_table_t pipelined;
    
    function Pipe_app_visitor_rest (
        p_vis_module_static_id VARCHAR2 DEFAULT 'Cloud_Visitors_Source'
    ) return appvisitor_table_t pipelined;
    
    PROCEDURE merge_remote_source_call (
        p_vis_module_static_id VARCHAR2 DEFAULT 'Cloud_Visitors_Source',
        p_geoloc_module_static_id VARCHAR2 DEFAULT 'IP_Geolocation'
    );

    PROCEDURE merge_remote_source (
        p_app_id NUMBER DEFAULT NV('APP_ID'),
        p_page_id NUMBER DEFAULT NV('APP_PAGE_ID'),
        p_user_name VARCHAR2 DEFAULT V('APP_USER'),
        p_vis_module_static_id VARCHAR2 DEFAULT 'Cloud_Visitors_Source',
        p_geoloc_module_static_id VARCHAR2 DEFAULT 'IP_Geolocation'
    );

    FUNCTION Web_Module_Job_Name (
        p_vis_module_static_id VARCHAR2 DEFAULT 'Cloud_Visitors_Source'
    ) return VARCHAR2;

    PROCEDURE Launch_merge_remote_Job (
        p_Enabled VARCHAR2 DEFAULT 'YES',
        p_app_id NUMBER DEFAULT NV('APP_ID'),
        p_page_id NUMBER DEFAULT NV('APP_PAGE_ID'),
        p_user_name VARCHAR2 DEFAULT V('APP_USER'),
        p_vis_module_static_id VARCHAR2 DEFAULT 'Cloud_Visitors_Source',
        p_geoloc_module_static_id VARCHAR2 DEFAULT 'IP_Geolocation'
    );
    
    PROCEDURE Define_RESTful_Service;
    
    PROCEDURE merge_local_source_call (
        p_geoloc_module_static_id VARCHAR2 DEFAULT 'IP_Geolocation'
    );

    PROCEDURE merge_local_source (
        p_app_id NUMBER DEFAULT NV('APP_ID'),
        p_page_id NUMBER DEFAULT NV('APP_PAGE_ID'),
        p_user_name VARCHAR2 DEFAULT V('APP_USER'),
        p_geoloc_module_static_id VARCHAR2 DEFAULT 'IP_Geolocation'
    );
    
    PROCEDURE Launch_merge_local_Job(
        p_Enabled VARCHAR2 DEFAULT 'YES',
        p_app_id NUMBER DEFAULT NV('APP_ID'),
        p_page_id NUMBER DEFAULT NV('APP_PAGE_ID'),
        p_user_name VARCHAR2 DEFAULT V('APP_USER'),
        p_geoloc_module_static_id VARCHAR2 DEFAULT 'IP_Geolocation'
    );

END CLOUD_VISITORS_UTL;
/

CREATE OR REPLACE PACKAGE BODY CLOUD_VISITORS_UTL 
IS
    -- at the endpoint --
    -- see RESTful Services / ORDS-RESTful Services / Module / appvisitors.rest

    function Pipe_Db_Ip_Geoloc_Rest (
        p_ipAddress VARCHAR2 DEFAULT 'self',
        p_geoloc_module_static_id VARCHAR2 DEFAULT 'IP_Geolocation',
        p_API_Key VARCHAR2 DEFAULT 'free'
    ) return db_ip_geoloc_free_table_t pipelined is
        l_columns apex_exec.t_columns;
        l_context apex_exec.t_context;
		l_columns_names apex_t_varchar2;
        l_parameters apex_exec.t_parameters;
        type t_column_position is table of pls_integer index by varchar2(32767);
        l_column_position t_column_position;
    begin
        if p_geoloc_module_static_id IS NULL then 
            return;
        end if;
        l_columns_names := apex_string.split(
        	'IPADDRESS,CONTINENTCODE,CONTINENTNAME,COUNTRYCODE,COUNTRYNAME,STATEPROV,CITY,ERROR,ERRORCODE'
        	, ',');

        -- specify columns to select from the web source module
		for c_idx IN 1..l_columns_names.count loop
			apex_exec.add_column( 
				p_columns       => l_columns,
				p_column_name   => l_columns_names(c_idx)  );
		end loop;
        apex_exec.add_parameter( 
            p_parameters => l_parameters, 
            p_name => 'apiKey', 
            p_value => p_API_Key );
        apex_exec.add_parameter( 
            p_parameters => l_parameters, 
            p_name => 'ipAddress', 
            p_value => p_ipAddress );

        -- invoke Web Source Module and select data
        l_context := apex_exec.open_web_source_query(
            p_module_static_id => p_geoloc_module_static_id,
            p_parameters       => l_parameters,
            p_columns          => l_columns );

        -- now get result set positions for the selected columns
		for c_idx IN 1..l_columns_names.count loop
			l_column_position( l_columns_names(c_idx) )  := apex_exec.get_column_position( l_context, l_columns_names(c_idx) );
		end loop;

        -- loop through result set and print out contents
        while apex_exec.next_row( l_context ) loop
            pipe row( 
                db_ip_geoloc_free_row_t(
                    apex_exec.get_varchar2( l_context, l_column_position( 'IPADDRESS' ) ),
                    apex_exec.get_varchar2( l_context, l_column_position( 'CONTINENTCODE' ) ),
                    apex_exec.get_varchar2( l_context, l_column_position( 'CONTINENTNAME' ) ),
                    apex_exec.get_varchar2( l_context, l_column_position( 'COUNTRYCODE' ) ),
                    apex_exec.get_varchar2( l_context, l_column_position( 'COUNTRYNAME' ) ),
                    apex_exec.get_varchar2( l_context, l_column_position( 'STATEPROV' ) ),
                    apex_exec.get_varchar2( l_context, l_column_position( 'CITY' ) ),
                    apex_exec.get_varchar2( l_context, l_column_position( 'ERROR' ) ),
                    apex_exec.get_varchar2( l_context, l_column_position( 'ERRORCODE' ) )
                ) 
            );
        end loop;

        -- finally: release all resources
        apex_exec.close( l_context );
    exception
    	when no_data_needed then
            apex_exec.close( l_context );    	
        when others then
        	if g_debug then
            	sys.dbms_output.put_line('Pipe_Db_Ip_Geoloc_Rest failed with error : ' || SQLERRM);
            end if;
            -- IMPORTANT: also release all resources, when an exception occcurs!
            apex_exec.close( l_context );
    end Pipe_Db_Ip_Geoloc_Rest;
    
    PROCEDURE Geoloc_Upd (
        p_geoloc_module_static_id VARCHAR2 DEFAULT 'IP_Geolocation'
    )
    as
        l_Row_Count NUMBER := 0;
        l_Row_Total NUMBER := 0;
    begin
        -- lookup geolocation of visitors ip address 
        for ip_cur in (
            SELECT DISTINCT 
                case when instr(ip_address, ',') > 0 then substr(ip_address, 1, instr(ip_address, ',')-1) else ip_address end IP_ADDRESS
            FROM CLOUD_VISITORS A
            WHERE IP_LOCATION IS NULL
        ) loop
        	if g_debug then
            	sys.dbms_output.put_line ('ip-Address is '||ip_cur.IP_ADDRESS);
            end if;
            l_Row_Count := 0;
            for ip_loc_cur in (
                select IP_ADDRESS, CONTINENTCODE, CONTINENTNAME, COUNTRYCODE, COUNTRYNAME, STATEPROV, CITY, ERRORCODE
                from table ( CLOUD_VISITORS_UTL.Pipe_Db_Ip_Geoloc_Rest (
                    p_ipAddress => ip_cur.IP_ADDRESS, 
                    p_geoloc_module_static_id => p_geoloc_module_static_id) ) S
            ) loop
            	exit when ip_loc_cur.ERRORCODE IS NOT NULL;
            	if ip_cur.IP_ADDRESS IS NOT NULL
            	and ip_loc_cur.CONTINENTCODE IS NOT NULL then
					UPDATE CLOUD_VISITORS 
					SET IP_LOCATION = ip_loc_cur.COUNTRYNAME ||', '|| ip_loc_cur.STATEPROV ||', '|| ip_loc_cur.CITY,
						CONTINENTCODE = ip_loc_cur.CONTINENTCODE,
						CONTINENTNAME = ip_loc_cur.CONTINENTNAME,
						COUNTRYCODE = ip_loc_cur.COUNTRYCODE,
						COUNTRYNAME = ip_loc_cur.COUNTRYNAME,
						STATEPROV = ip_loc_cur.STATEPROV,
						CITY = ip_loc_cur.CITY
					WHERE IP_LOCATION IS NULL
					AND IP_ADDRESS LIKE ip_cur.IP_ADDRESS || '%';
					-- the rest access task may fail any time. So let´s save what we have.
					l_Row_Count := l_Row_Count + SQL%ROWCOUNT;
					COMMIT;
				end if;
            end loop;
            exit when l_Row_Count = 0;
            l_Row_Total := l_Row_Total + l_Row_Count;
        end loop;
        if g_debug then
        	sys.dbms_output.put_line ('merged ' || l_Row_Total || ' geolocation rows');
        end if;
        COMMIT;
    end Geoloc_Upd;

    PROCEDURE Geoloc_Upd (
        p_app_id NUMBER,
        p_page_id NUMBER,
        p_user_name VARCHAR2,
        p_geoloc_module_static_id VARCHAR2 DEFAULT 'IP_Geolocation'
    )
    as
        l_Row_Count NUMBER := 0;
    begin
        apex_session.create_session ( 
            p_app_id => p_app_id,
            p_page_id => p_page_id,
            p_username => p_user_name
        );
    	if g_debug then
        	sys.dbms_output.enable(null);
	        sys.dbms_output.put_line ('App is '||v('APP_ID')|| ', session is ' || v('APP_SESSION'));
        end if;
        -- lookup geolocation of visitors ip address 
        Geoloc_Upd(p_geoloc_module_static_id=>p_geoloc_module_static_id);
        apex_session.delete_session ( p_session_id => v('APP_SESSION') );
    end Geoloc_Upd;

/* 
-- at the callers point --
Define a Data Sources / Web Source Module "Data Browser Visitors Source"
*/

    function Pipe_app_visitor_rest (
        p_vis_module_static_id VARCHAR2 DEFAULT 'Cloud_Visitors_Source'
    ) return appvisitor_table_t pipelined 
    is
        l_columns apex_exec.t_columns;
        l_context apex_exec.t_context;
		l_columns_names apex_t_varchar2;
        type t_column_position is table of pls_integer index by varchar2(32767);
        l_column_position t_column_position;
    begin
        if p_vis_module_static_id IS NULL then 
            return;
        end if;
        l_columns_names := apex_string.split(
        	'LAST_LOGIN_DATE,LOGIN_CNT,APPLICATION_ID,PAGE_ID,APPLICATION_NAME,APEX_USER,IP_ADDRESS,AGENT,PAGE_NAME,REQUESTS,CNT,ELAPSED_TIME,DURATION_MINS'
        	, ',');
        -- specify columns to select from the web source module
		for c_idx IN 1..l_columns_names.count loop
			apex_exec.add_column( 
				p_columns       => l_columns,
				p_column_name   => l_columns_names(c_idx)  );
		end loop;
		
        -- invoke Web Source Module and select data
        l_context := apex_exec.open_web_source_query(
            p_module_static_id => p_vis_module_static_id,
            p_columns          => l_columns );

        -- now get result set positions for the selected columns
		for c_idx IN 1..l_columns_names.count loop
			l_column_position( l_columns_names(c_idx) )  := apex_exec.get_column_position( l_context, l_columns_names(c_idx) );
		end loop;

        -- loop through result set and print out contents
        while apex_exec.next_row( l_context ) loop
            pipe row( 
                appvisitor_row_t(
                    apex_exec.get_varchar2( l_context, l_column_position( 'LAST_LOGIN_DATE' ) ),
                    apex_exec.get_number( l_context, l_column_position( 'LOGIN_CNT' ) ),
                    apex_exec.get_number( l_context, l_column_position( 'APPLICATION_ID' ) ),
                    apex_exec.get_number( l_context, l_column_position( 'PAGE_ID' ) ),
                    apex_exec.get_varchar2( l_context, l_column_position( 'APPLICATION_NAME' ) ),
                    apex_exec.get_varchar2( l_context, l_column_position( 'APEX_USER' ) ),
                    apex_exec.get_varchar2( l_context, l_column_position( 'IP_ADDRESS' ) ),
                    apex_exec.get_varchar2( l_context, l_column_position( 'AGENT' ) ),
                    apex_exec.get_varchar2( l_context, l_column_position( 'PAGE_NAME' ) ),
                    apex_exec.get_varchar2( l_context, l_column_position( 'REQUESTS' ) ),
                    apex_exec.get_number( l_context, l_column_position( 'CNT' ) ),
                    apex_exec.get_varchar2( l_context, l_column_position( 'ELAPSED_TIME' ) ),
                    apex_exec.get_varchar2( l_context, l_column_position( 'DURATION_MINS' ) )
                ) 
            );
        end loop;

        -- finally: release all resources
        apex_exec.close( l_context );
    exception
    	when no_data_needed then
            apex_exec.close( l_context );    	
        when others then
        	if g_debug then
            	sys.dbms_output.put_line('Pipe_app_visitor_rest failed with error : ' || SQLERRM);
            end if;
            -- IMPORTANT: also release all resources, when an exception occcurs!
            apex_exec.close( l_context );
            raise;
    end Pipe_app_visitor_rest;

    PROCEDURE merge_remote_source_call (
        p_vis_module_static_id VARCHAR2 DEFAULT 'Cloud_Visitors_Source',
        p_geoloc_module_static_id VARCHAR2 DEFAULT 'IP_Geolocation'
    ) 
    is 
    begin 
        merge into CLOUD_VISITORS D 
        using (
            select  p_vis_module_static_id WEB_MODULE_ID,
                    LAST_LOGIN_DATE, 
                    SUM(LOGIN_CNT) LOGIN_CNT, 
                    APPLICATION_ID, PAGE_ID,
                    MAX(APPLICATION_NAME) APPLICATION_NAME, 
                    MAX(APEX_USER) APEX_USER, 
                    IP_ADDRESS, 
                    MAX(AGENT) AGENT, 
                    MAX(PAGE_NAME) PAGE_NAME, 
                    MAX(REQUESTS) REQUESTS, 
                    SUM(CNT) CNT,
                    MAX(ELAPSED_TIME) ELAPSED_TIME, 
                    MAX(DURATION_MINS) DURATION_MINS
            from table ( CLOUD_VISITORS_UTL.Pipe_app_visitor_rest (p_vis_module_static_id) )
            group by LAST_LOGIN_DATE, IP_ADDRESS, APPLICATION_ID, PAGE_ID
        ) S 
        on (D.WEB_MODULE_ID = S.WEB_MODULE_ID and D.LAST_LOGIN_DATE = S.LAST_LOGIN_DATE and D.IP_ADDRESS = S.IP_ADDRESS and D.APPLICATION_ID = S.APPLICATION_ID and D.PAGE_ID = S.PAGE_ID) 
        when matched then 
            update set D.AGENT = S.AGENT, D.LOGIN_CNT = S.LOGIN_CNT, D.PAGE_NAME = S.PAGE_NAME, D.REQUESTS = S.REQUESTS, 
                        D.ELAPSED_TIME = S.ELAPSED_TIME, D.DURATION_MINS = S.DURATION_MINS
        when not matched then 
            insert (D.WEB_MODULE_ID, D.LAST_LOGIN_DATE, D.LOGIN_CNT, D.APPLICATION_ID, D.PAGE_ID, 
            		D.APPLICATION_NAME, D.APEX_USER, D.IP_ADDRESS, D.AGENT, 
                    D.PAGE_NAME, D.REQUESTS, D.CNT, D.ELAPSED_TIME, D.DURATION_MINS)
            values (S.WEB_MODULE_ID, S.LAST_LOGIN_DATE, S.LOGIN_CNT, S.APPLICATION_ID, S.PAGE_ID, 
            		S.APPLICATION_NAME, S.APEX_USER, S.IP_ADDRESS, S.AGENT, 
                    S.PAGE_NAME, S.REQUESTS, S.CNT, S.ELAPSED_TIME, S.DURATION_MINS)
        ;
        if g_debug then
        	sys.dbms_output.put_line ('merge_remote_source merged ' || SQL%ROWCOUNT || ' rows');
        end if;
        commit;
        
        merge into CLOUD_VISITORS_EXCLUDED_IP_LIST D 
        using (
            select 
                IP_ADDRESS 
            from table (CLOUD_VISITORS_UTL.Pipe_Db_Ip_Geoloc_Rest( 
                p_ipAddress=>'self', 
                p_geoloc_module_static_id=>p_geoloc_module_static_id
            ))
            where CITY IS NOT NULL
        ) S
        on (D.IP_ADDRESS = S.IP_ADDRESS)
        when not matched then 
            insert (D.IP_ADDRESS)
            values (S.IP_ADDRESS)
        ;   
        commit;
        Geoloc_Upd(p_geoloc_module_static_id=>p_geoloc_module_static_id);
    end merge_remote_source_call; 
    
    PROCEDURE merge_remote_source (
        p_app_id NUMBER DEFAULT NV('APP_ID'),
        p_page_id NUMBER DEFAULT NV('APP_PAGE_ID'),
        p_user_name VARCHAR2 DEFAULT V('APP_USER'),
        p_vis_module_static_id VARCHAR2 DEFAULT 'Cloud_Visitors_Source',
        p_geoloc_module_static_id VARCHAR2 DEFAULT 'IP_Geolocation'
    )
    is
    begin
        apex_session.create_session ( 
            p_app_id => p_app_id,
            p_page_id => p_page_id,
            p_username => p_user_name
        );
        if g_debug then
        	sys.dbms_output.enable(null);
        	sys.dbms_output.put_line ('App is '||v('APP_ID')|| ', session is ' || v('APP_SESSION'));
        end if;
        merge_remote_source_call (
            p_vis_module_static_id => p_vis_module_static_id,
            p_geoloc_module_static_id => p_geoloc_module_static_id );
        apex_session.delete_session ( p_session_id => v('APP_SESSION') );
    end merge_remote_source;

    FUNCTION Web_Module_Job_Name (
        p_vis_module_static_id VARCHAR2 DEFAULT 'Cloud_Visitors_Source'
    ) return VARCHAR2
    is 
    begin 
        return UPPER(SUBSTR('DBR_VIS_' || p_vis_module_static_id, 1, 64));
    end Web_Module_Job_Name;
    
    PROCEDURE Launch_merge_remote_Job (
        p_Enabled VARCHAR2 DEFAULT 'YES',
        p_app_id NUMBER DEFAULT NV('APP_ID'),
        p_page_id NUMBER DEFAULT NV('APP_PAGE_ID'),
        p_user_name VARCHAR2 DEFAULT V('APP_USER'),
        p_vis_module_static_id VARCHAR2 DEFAULT 'Cloud_Visitors_Source',
        p_geoloc_module_static_id VARCHAR2 DEFAULT 'IP_Geolocation'
    )
    is 
        v_Job_Name USER_SCHEDULER_JOBS.JOB_NAME%TYPE := Web_Module_Job_Name(p_vis_module_static_id);
        v_Job_STATE USER_SCHEDULER_JOBS.STATE%TYPE;
        v_By_Minute NUMBER;
    begin
        begin
            SELECT STATE, JOB_NAME
            INTO v_Job_STATE, v_Job_Name
            FROM USER_SCHEDULER_JOBS
            WHERE JOB_NAME LIKE v_Job_Name || '%';
            if g_debug then
            	DBMS_OUTPUT.PUT_LINE('Job - found ' || v_Job_Name || ', state: ' || v_Job_STATE );
            end if;
            if p_Enabled = 'NO' then
                if v_Job_STATE = 'RUNNING' then 
                    dbms_scheduler.stop_job ( job_name => v_Job_Name );
                end if;
                dbms_scheduler.drop_job (
                    job_name => v_Job_Name,
                    force => TRUE
                );
                commit;
                if g_debug then
                	DBMS_OUTPUT.PUT_LINE('Job - stopped ' || v_Job_Name );
                end if;
            end if;
        exception
          when NO_DATA_FOUND then
            null;
        end;
        if p_Enabled = 'YES' then
            DBMS_SCHEDULER.CREATE_JOB (
               job_name        => v_Job_Name
              ,repeat_interval => 'FREQ=DAILY;BYHOUR=3,9,12,15,18,21;BYMINUTE=15;'
              ,job_class       => 'DEFAULT_JOB_CLASS'
              ,job_type        => 'PLSQL_BLOCK'
              ,job_action      => 'begin  CLOUD_VISITORS_UTL.merge_remote_source(p_app_id=>' || DBMS_ASSERT.ENQUOTE_LITERAL(p_app_id) 
                                || ',p_page_id=>' || DBMS_ASSERT.ENQUOTE_LITERAL(p_page_id) 
                                || ',p_user_name=>' || DBMS_ASSERT.ENQUOTE_LITERAL(p_user_name) 
                                || ',p_vis_module_static_id=>' || DBMS_ASSERT.ENQUOTE_LITERAL(p_vis_module_static_id) 
                                || ',p_geoloc_module_static_id=>' || DBMS_ASSERT.ENQUOTE_LITERAL(p_geoloc_module_static_id) 
                                || '); end;'
              ,comments        => 'Job to refresh the recent visitors list.'
              ,enabled         => TRUE
            );  
            commit;
        end if;
    end Launch_merge_remote_Job;


    PROCEDURE Define_RESTful_Service
    IS 
    begin
        ords.enable_schema;

        ords.delete_module(
            p_module_name => 'appvisitors.rest' );


        ords.define_module(
            p_module_name => 'appvisitors.rest',
            p_base_path => '/appvisitors/' );

        ords.define_template(
            p_module_name => 'appvisitors.rest',
            p_pattern     => 'hol/' );

        ords.define_template(
            p_module_name => 'appvisitors.rest',
            p_pattern     => 'hol/:ip_address' );

        ords.define_handler(
            p_module_name => 'appvisitors.rest',
            p_pattern     => 'hol/',
            p_method      => 'GET',
            p_source_type => ords.source_type_collection_feed,
            p_source      => 'select * from CLOUD_VISITORS_V' );

        ords.define_handler(
            p_module_name => 'appvisitors.rest',
            p_pattern     => 'hol/:ip_address',
            p_method      => 'GET',
            p_source_type => ords.source_type_collection_item,
            p_source      => 'select * from CLOUD_VISITORS_V where ip_address = :ip_address' );
        commit;
    end Define_RESTful_Service;
        
    PROCEDURE merge_local_source_call (
        p_geoloc_module_static_id VARCHAR2 DEFAULT 'IP_Geolocation'
    )
    is 
    begin
        merge into CLOUD_VISITORS D 
        using (
            select 'localhost' WEB_MODULE_ID,
                    LAST_LOGIN_DATE, 
                    SUM(LOGIN_CNT) LOGIN_CNT, 
                    APPLICATION_ID, PAGE_ID,
                    MAX(APPLICATION_NAME) APPLICATION_NAME, 
                    IP_ADDRESS, 
                    MAX(AGENT) AGENT, 
                    MAX(PAGE_NAME) PAGE_NAME, 
                    MAX(REQUESTS) REQUESTS, 
                    SUM(CNT) CNT,
                    MAX(ELAPSED_TIME) ELAPSED_TIME, 
                    MAX(DURATION_MINS) DURATION_MINS
            from CLOUD_VISITORS_V
            group by LAST_LOGIN_DATE, IP_ADDRESS, APPLICATION_ID, PAGE_ID
        ) S 
        on (D.WEB_MODULE_ID = S.WEB_MODULE_ID and D.LAST_LOGIN_DATE = S.LAST_LOGIN_DATE and D.IP_ADDRESS = S.IP_ADDRESS and D.APPLICATION_ID = S.APPLICATION_ID and D.PAGE_ID = S.PAGE_ID) 
        when matched then 
            update set D.AGENT = S.AGENT, D.LOGIN_CNT = S.LOGIN_CNT, D.PAGE_NAME = S.PAGE_NAME, D.REQUESTS = S.REQUESTS, 
                    D.ELAPSED_TIME = S.ELAPSED_TIME, D.DURATION_MINS = S.DURATION_MINS
        when not matched then 
            insert (D.WEB_MODULE_ID, D.LAST_LOGIN_DATE, D.LOGIN_CNT, D.APPLICATION_ID, D.PAGE_ID, D.APPLICATION_NAME, D.IP_ADDRESS, D.AGENT, 
                    D.PAGE_NAME, D.REQUESTS, D.CNT, D.ELAPSED_TIME, D.DURATION_MINS)
            values (S.WEB_MODULE_ID, S.LAST_LOGIN_DATE, S.LOGIN_CNT, S.APPLICATION_ID, S.PAGE_ID, S.APPLICATION_NAME, S.IP_ADDRESS, S.AGENT, 
                    S.PAGE_NAME, S.REQUESTS, S.CNT, S.ELAPSED_TIME, S.DURATION_MINS)
        ;
        commit;
        Geoloc_Upd(p_geoloc_module_static_id);
    end merge_local_source_call;

    PROCEDURE merge_local_source (
        p_app_id NUMBER DEFAULT NV('APP_ID'),
        p_page_id NUMBER DEFAULT NV('APP_PAGE_ID'),
        p_user_name VARCHAR2 DEFAULT V('APP_USER'),
        p_geoloc_module_static_id VARCHAR2 DEFAULT 'IP_Geolocation'
    )
    is 
    begin
        apex_session.create_session ( 
            p_app_id => p_app_id,
            p_page_id => p_page_id,
            p_username => p_user_name
        );
        if g_debug then
    	    sys.dbms_output.put_line ('App is '||v('APP_ID')|| ', session is ' || v('APP_SESSION'));
    	end if;
        merge_local_source_call(p_geoloc_module_static_id);
        apex_session.delete_session ( p_session_id => v('APP_SESSION') );
    end merge_local_source;

    PROCEDURE Launch_merge_local_Job(
        p_Enabled VARCHAR2 DEFAULT 'YES',
        p_app_id NUMBER DEFAULT NV('APP_ID'),
        p_page_id NUMBER DEFAULT NV('APP_PAGE_ID'),
        p_user_name VARCHAR2 DEFAULT V('APP_USER'),
        p_geoloc_module_static_id VARCHAR2 DEFAULT 'IP_Geolocation'
    )
    is 
        v_Job_Name CONSTANT USER_SCHEDULER_JOBS.JOB_NAME%TYPE := Web_Module_Job_Name('LOCALHOST');
        v_Job_STATE USER_SCHEDULER_JOBS.STATE%TYPE;
    begin
        begin
            SELECT STATE
            INTO v_Job_STATE
            FROM USER_SCHEDULER_JOBS
            WHERE JOB_NAME = v_Job_Name;
            if g_debug then
            	DBMS_OUTPUT.PUT_LINE('Job - found ' || v_Job_Name || ', state: ' || v_Job_STATE );
            end if;
            if p_Enabled = 'NO' then
                if v_Job_STATE = 'RUNNING' then 
                    dbms_scheduler.stop_job ( job_name => v_Job_Name );
                end if;
                dbms_scheduler.drop_job (
                    job_name => v_Job_Name,
                    force => TRUE
                );
                commit;
                if g_debug then
                	DBMS_OUTPUT.PUT_LINE('Job - stopped ' || v_Job_Name );
                end if;
            end if;
        exception
          when NO_DATA_FOUND then
            null;
        end;
        if p_Enabled = 'YES' then
            DBMS_SCHEDULER.CREATE_JOB (
               job_name        => v_Job_Name
              ,repeat_interval => 'FREQ=DAILY;BYHOUR=3,9,12,15,18,21;BYMINUTE=5;'
              ,job_class       => 'DEFAULT_JOB_CLASS'
              ,job_type        => 'PLSQL_BLOCK'
              ,job_action      => 'begin  CLOUD_VISITORS_UTL.merge_local_source(p_app_id=>' || DBMS_ASSERT.ENQUOTE_LITERAL(p_app_id) 
                                || ',p_page_id=>' || DBMS_ASSERT.ENQUOTE_LITERAL(p_page_id) 
                                || ',p_user_name=>' || DBMS_ASSERT.ENQUOTE_LITERAL(p_user_name) 
                                || ',p_geoloc_module_static_id=>' || DBMS_ASSERT.ENQUOTE_LITERAL(p_geoloc_module_static_id) 
                                || '); end;'
              ,comments        => 'Job to refresh the recent visitors list.'
              ,enabled         => TRUE
            );  
            commit;
        end if;
    end Launch_merge_local_Job;
        
END CLOUD_VISITORS_UTL;
/

begin
    CLOUD_VISITORS_UTL.Define_RESTful_Service;
end;
/

/*
set serveroutput on size unlimited
begin 
    CLOUD_VISITORS_UTL.Launch_merge_remote_Job(
        p_app_id=>2050, 
        p_page_id=>7, 
        p_user_name=>USER, 
        p_Enabled=>'YES', 
        p_vis_module_static_id=>'Strack_Software_dev_App_Visitors'
    ); 
end;
/
begin CLOUD_VISITORS_UTL.merge_remote_source(p_app_id=>2050, p_page_id=>7, p_user_name=>USER); end;
/
begin CLOUD_VISITORS_UTL.Launch_merge_local_Job(p_app_id=>2000, p_page_id=>1, p_user_name=>USER); end;
/

*/