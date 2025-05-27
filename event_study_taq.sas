options noserror nosource nonotes nostimer;

* --- MACRO VARIABLES FOR USER CONFIGURATION --- ;
* --- Input Event Data Configuration --- ;
%let event_lib = WORK; /* Libref for your input event SAS dataset */
%let event_ds = your_event_data; /* Name of your input event SAS dataset (.sas7bdat format) */
%let event_id_col = ticker; /* Name of the column in your event data that contains the stock identifier (e.g., ticker, cusip) */
%let event_year_filter_active = 1；
%let event_year_filter_threshold = 2014; /* There's difference in TAQ between before and after 2014 */

* --- Output Configuration --- ;
%let save_option = SASDATASET; /* Choose output method: 'SASDATASET' or 'DOWNLOAD' */
%let output_lib = WORK; /* Libref for the final output SAS dataset (if save_option = 'SASDATASET') */
%let output_ds = event_impact_analysis; /* Name of the final output SAS dataset (if save_option = 'SASDATASET') */

%let output_lib_download = USER; /* Libref for PROC DOWNLOAD target - if save_option = 'DOWNLOAD' */
%let output_ds_download = event_impact_analysis_dl; /* Filename for PROC DOWNLOAD - if save_option = 'DOWNLOAD' */


data trans (rename=(&event_id_col.=ticker));
    set &event_lib..&event_ds.;

    format etimestamp e8601dt19.;
    date = datepart(etimestamp);
    format date yymmdd10.;
run;

%if &event_year_filter_active. = 1 %then %do;
    data trans;
        set trans;
        if year(date) < &event_year_filter_threshold.;
    run;
%end;

proc sort data=trans(keep=date ticker gvkey cusip permno etimestamp)
    out=datelist_full nodupkey;
    by date;
run;

data datelist;
    set datelist_full;
    by date;
    if first.date;
    keep date;
run;

data trans_taq;
    length gvkey 8 cusip $9 permno 8 ticker $10
           date 8 starttime2 8 endtime2 8 datetime_s 8 wvprice_s 8
           datetime_e 8 wvprice_e 8 volume 8 trade 8 sym $10
           datetime_q_s 8 bid_s 8 bidsiz_s 8 ask_s 8 asksiz_s 8
           datetime_q_e 8 bid_e 8 bidsiz_e 8 ask_e 8 asksiz_e 8;

    format date date9.
           starttime2 endtime2 datetime_s datetime_e datetime_q_s datetime_q_e e8601dt19.;
    stop;
run;

data dcheck;
    length date 8 i 8;
run;

proc sql;
    create table busd as
    select memname as filename
    from dictionary.tables
    where libname='TAQMSEC' and memname like 'CTM_%';
quit;

data busd;
    set busd;
    date = input(substr(filename, 5), yymmdd8.);
    format date yymmdd10.;
    filename1 = lag(date);
    format filename1 yymmdd10.;
run;

* --- MACRO FOR PROCESSING DATA FOR EACH EVENT DATE --- ;
%macro process_data(start=1, end=0);

    %local total_rows current_loop_end;

    proc sql noprint;
        select count(*) into :total_rows
        from datelist;
    quit;

    %if &end. = 0 or &end. > &total_rows. %then %do;
        %let current_loop_end = &total_rows.;
    %end;
    %else %do;
        %let current_loop_end = &end.;
    %end;

    %do i = &start. %to &current_loop_end.;

        %local startf startf_sql startf1;

        proc sql;
            create table temp_datelist_joined as
            select a.date, b.filename1 as date1
            from datelist(firstobs=&i obs=&i) as a
            left join busd as b
            on a.date = b.date;
        quit;

        data _null_;
            set temp_datelist_joined;
            call symput('startf', put(date, yymmddn8.));
            call symput('startf_sql', put(date, date9.));
            call symput('startf1', put(date1, yymmddn8.));
        run;

        options source notes stimer;
        %put Processing Date: &&startf_sql., Iteration: &i / &total_rows.;
        options nosource nonotes nostimer;

        %if %symexist(startf1) and %length(&&startf1) > 0 %then %do;
        %end;
        %else %do;
            %put NOTE: Previous day TAQ data (startf1) not found for date &&startf_sql.. Skipping this date.;
            %goto continue_loop;
        %end;

        /* proc sql noprint; */
        /* select count(*) */
        /* into :indexs_exists */
        /* from busd */
        /* where date="&startf_sql"d; */
        /* quit; */
        /* */
        /* %if %symexist(indexs_exists) %then %do; */
        /* %if &indexs_exists = 0 %then %do; */
        /* data dcheck; */
        /* set dcheck; */
        /* i = &i.; */
        /* date = "&startf_sql"d; */
        /* output; */
        /* run; */
        /* %goto continue_loop; */ /* Go to end of loop iteration if date not in busd */
        /* %end; */
        /* %end; */

        proc sql;
            create table tab as
            select *
            from trans
            where date = "&&startf_sql."d;
        quit;

        proc sql;
            create table taq as
            select *,
                   catx('', sym_root, sym_suffix) as SYM /* Create full symbol */
            from taqmsec.ctm_&startf. (where=(price > 0 and size > 0 and tr_corr in ('00', '01', '02')))
            where sym_root in (select distinct ticker from tab);
        quit;

        proc sql;
            create table taq_1 as
            select *,
                   catx('', sym_root, sym_suffix) as SYM
            from taqmsec.ctm_&startf1. (where=(price > 0 and size > 0 and tr_corr in ('00', '01', '02')))
            where sym_root in (select distinct ticker from tab);
        quit;

        data taq;
            set taq taq_1;
        run;

        proc sql;
            create table taq as
            select distinct *
            from taq;
        quit;

        data taq;
            set taq;
            datetime_str = catx(' ', put(date, yymmdd10.), put(time_m, time8.));
            datetime_str = scan(datetime_str, 1, '.');
            DATETIME = input(datetime_str, anydtdtm.);
            drop datetime_str;
            format datetime e8601dt19.;
        run;

        data taq;
            set taq;
            tr_scond_sub = strip(substr(tr_scond, 1, 2));
            if tr_scond_sub in ('', ' ', '@', '*', 'E', 'F', '@E', '@F', '*E', '*F');
        run;

        proc sql;
            create table taq as
            select distinct * from taq;
        quit;

        proc sql;
            create table taq as
            select sym_root, sym_suffix, sym, datetime, size, price
            from taq;
        quit;

        proc sql;
            create table vol_trd as
            select sym,
                   datetime,
                   sum(size) as VOLUME,
                   count(*) as TRADE
            from taq
            group by sym, datetime;
        quit;

        proc sql;
            create table taq as
            select a.*,
                   b.VOLUME,
                   b.TRADE
            from taq as a
            left join vol_trd as b
            on a.sym = b.sym and a.datetime = b.datetime
            order by a.sym, a.datetime;
        quit;

        proc sql;
            create table taq as
            select sym_root,
                   sym_suffix,
                   sym,
                   datetime,
                   price,
                   size,
                   volume,
                   trade,
                   (size/volume)*price as WVPRICE_component
            from taq;
        quit;

        proc sql;
            create table wvprice_agg as
            select sym,
                   datetime,
                   sum(WVPRICE_component) as WVPRICE
            from taq
            group by sym, datetime;
        quit;

        proc sql;
            create table taq as
            select a.sym_root,
                   a.sym_suffix,
                   a.sym,
                   a.datetime,
                   a.price,
                   a.size,
                   a.volume,
                   a.trade,
                   b.wvprice
            from taq as a
            left join wvprice_agg as b
            on a.sym = b.sym and a.datetime = b.datetime
            order by a.sym, a.datetime;
        quit;

        proc sql;
            create table taq as
            select distinct sym_root, sym_suffix, sym, datetime, volume, trade, wvprice
            from taq;
        quit;

        data tab;
            set tab;
            starttime2 = etimestamp;
            endtime2 = intnx('minute', starttime2, 15);
            format starttime2 e8601dt19.;
            format endtime2 e8601dt19.;
        run;

        proc sql;
            create table tab_s as
            select a.*,
                   b.datetime as temp_datetime_s,
                   b.wvprice as temp_wvprice_s
            from tab as a
            left join taq as b
            on a.ticker = b.sym
            and a.starttime2 >= b.datetime
            group by a.gvkey, a.cusip, a.permno, a.ticker, a.starttime2
            having b.datetime = max(b.datetime);
            /* order by a.ticker, a.starttime2, b.datetime desc;
        quit;

        data tab;
            set tab_s;
            datetime_s = temp_datetime_s;
            wvprice_s  = temp_wvprice_s;
            drop temp_datetime_s temp_wvprice_s;
        run;

        proc sql;
            create table tab_e as
            select a.*,
                   b.datetime as temp_datetime_e,
                   b.wvprice as temp_wvprice_e
            from tab as a
            left join taq as b
            on a.ticker = b.sym
            and a.endtime2 >= b.datetime
            group by a.gvkey, a.cusip, a.permno, a.ticker, a.starttime2, a.endtime2
            having b.datetime = max(b.datetime);
        quit;

        data tab;
            set tab_e;
            datetime_e = temp_datetime_e;
            wvprice_e = temp_wvprice_e;
            drop temp_datetime_e temp_wvprice_e;
        run;

        proc sql;
            create table tab_vol_trade_window as
            select a.gvkey, a.cusip, a.permno, a.ticker, a.date, a.starttime2, a.endtime2,
                   sum(b.volume) as volume,
                   sum(b.trade) as trade
            from tab as a
            left join taq as b
            on a.ticker = b.sym
            and b.datetime >= a.starttime2 and b.datetime <= a.endtime2
            group by a.gvkey, a.cusip, a.permno, a.ticker, a.date, a.starttime2, a.endtime2;
        quit;

        proc sql;
            create table tab_final_trades as
            select a.*,
                   b.volume,
                   b.trade
            from tab as a
            left join tab_vol_trade_window as b
            on a.gvkey = b.gvkey and a.cusip = b.cusip and a.permno = b.permno
            and a.ticker = b.ticker and a.date = b.date
            and a.starttime2 = b.starttime2 and a.endtime2 = b.endtime2;
        quit;

        proc sql;
            create table taq_q as
            select date,
                   time_m,
                   sym_root,
                   sym_suffix,
                   bid,
                   bidsiz,
                   ask,
                   asksiz,
                   catx('', sym_root, sym_suffix) as SYM,
                   ((ask - bid) / ((ask + bid) / 2)) as relative_spread
            from taqmsec.cqm_&startf.
            where sym_root in (select distinct ticker from tab_final_trades)
            having relative_spread <= 0.2 and relative_spread > 0;
        quit;

        data taq_q;
            set taq_q;
            datetime_str = catx(' ', put(date, yymmdd10.), put(time_m, time8.));
            datetime_str = scan(datetime_str, 1, '.');
            DATETIME = input(datetime_str, anydtdtm.);
            drop datetime_str;
            format datetime e8601dt19.;
        run;

        proc sql;
            create table taq_q_s_prep as
            select sym,
                   date as date_q_s,
                   time_m as time_q_s,
                   datetime as datetime_q_s,
                   bid as bid_s,
                   bidsiz as bidsiz_s,
                   ask as ask_s,
                   asksiz as asksiz_s
            from taq_q
            order by sym, datetime_q_s;
        quit;

        proc sql;
            create table tab_quotes_s as
            select a.*,
                   b.datetime_q_s, b.bid_s, b.bidsiz_s, b.ask_s, b.asksiz_s,
                   b.date_q_s, b.time_q_s
            from tab_final_trades as a
            left join taq_q_s_prep as b
            on a.ticker = b.sym
            and a.starttime2 >= b.datetime_q_s
            group by a.gvkey, a.cusip, a.permno, a.ticker, a.starttime2
            having b.time_m = max(b.time_m);
        quit;

        proc sql;
            create table taq_q_e_prep as
            select sym,
                   date as date_q_e,
                   time_m as time_q_e,
                   datetime as datetime_q_e,
                   bid as bid_e,
                   bidsiz as bidsiz_e,
                   ask as ask_e,
                   asksiz as asksiz_e
            from taq_q
            order by sym, datetime_q_e;
        quit;

        proc sql;
            create table tab_final_quotes as
            select a.*,
                   b.datetime_q_e, b.bid_e, b.bidsiz_e, b.ask_e, b.asksiz_e,
                   b.date_q_e, b.time_q_e
            from tab_quotes_s as a
            left join taq_q_e_prep as b
            on a.ticker = b.sym
            and a.endtime2 >= b.datetime_q_e
            group by a.gvkey, a.cusip, a.permno, a.ticker, a.starttime2, a.endtime2
            having b.time_m = max(b.time_m);
        quit;

        proc sort data=tab_final_quotes out=tab_presort nodupkey;
             by _all_;
        run;

        proc sql;
            create table tab_to_append as
            select *
            from (
                select *,
                       monotonic() as row_num
                from tab_presort
                group by gvkey, cusip, permno, ticker, date, starttime2, endtime2
            )
            group by gvkey, cusip, permno, ticker, date, starttime2, endtime2
            having row_num = min(row_num);
        quit;

        proc sql;
            create table tab_for_append_final as
            select
                a.gvkey, a.cusip, a.permno, a.ticker,
                a.date, a.starttime2, a.endtime2,
                a.datetime_s, a.wvprice_s,
                a.datetime_e, a.wvprice_e,
                a.volume, a.trade,
                b.sym,
                a.datetime_q_s, a.bid_s, a.bidsiz_s, a.ask_s, a.asksiz_s,
                a.datetime_q_e, a.bid_e, a.bidsiz_e, a.ask_e, a.asksiz_e
            from tab_to_append a
            left join taq_q b on a.ticker = b.sym and a.datetime_q_s = b.datetime;
        quit;

        data tab_for_append_final_cols;
            set tab_for_append_final;
            if sym = "" then sym = ticker;
            keep gvkey cusip permno ticker date starttime2 endtime2 datetime_s wvprice_s
                 datetime_e wvprice_e volume trade sym
                 datetime_q_s bid_s bidsiz_s ask_s asksiz_s
                 datetime_q_e bid_e bidsiz_e ask_e asksiz_e;
        run;

        proc append base=trans_taq data=tab_for_append_final_cols force;
        run;

        %if "&save_option." = "DOWNLOAD" and %sysfunc(mod(&i., 10)) = 0 %then %do;
            proc download data=trans_taq out="&output_lib_download..&output_ds_download.";
            run;
            %put NOTE: Intermediate results downloaded at iteration &i.;
        %end;

        %continue_loop:
        %put NOTE: Finished processing for date &&startf_sql.;
        options source notes stimer;
        %put NOTE: Iteration &i completed. Memory used: %sysfunc(getoption(memused)/1024/1024, 8.2) MB.;
        options nosource nonotes nostimer;


    %end;

    options source notes stimer;

%mend process_data;

* --- EXECUTE THE MACRO --- ;
%process_data;

* --- Output Handling --- ;
%if "&save_option." = "SASDATASET" %then %do;
    * --- Save the final 'trans_taq' dataset to a permanent SAS library --- ;
    data &output_lib..&output_ds.;
        set trans_taq;
    run;
    %put NOTE: Results saved to &output_lib..&output_ds.;
%end;
%else %if "&save_option." = "DOWNLOAD" %then %do;
     %if %sysfunc(mod(&current_loop_end., 10)) ne 0 %then %do;
        proc download data=trans_taq out="&output_lib_download..&output_ds_download.";
        run;
        %put NOTE: Results downloaded to &output_lib_download..&output_ds_download.;
     %end;
%end;