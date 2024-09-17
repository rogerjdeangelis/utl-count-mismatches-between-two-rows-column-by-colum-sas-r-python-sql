# utl-count-mismatches-between-two-rows-column-by-colum-sas-r-python-sql
    %let pgm=utl-count-mismatches-between-two-rows-column-by-colum-sas-r-python-sql;

       SOLUTIONS

             1 sas datastep
             2 sas sql w/wo arrays
             3 r sql
             4 python sql w/wo arrays

    github
    https://tinyurl.com/8pxjptn4
    https://github.com/rogerjdeangelis/utl-count-mismatches-between-two-rows-column-by-colum-sas-r-python-sql

    stackoverflow r
    https://tinyurl.com/ycbjxfkb
    https://stackoverflow.com/questions/78933594/how-to-count-mismatches-between-two-rows-column-by-column

    /*               _     _
     _ __  _ __ ___ | |__ | | ___ _ __ ___
    | `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
    | |_) | | | (_) | |_) | |  __/ | | | | |
    | .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
    |_|
    */

    /**************************************************************************************************************************/
    /*                                          |                                                  |                          */
    /*           INPUT                          |             PROCESS                              |   OUTPUT                 */
    /*                                          |                                                  |                          */
    /*                                          |                                                  |                          */
    /* GRP  ID  X1  X2  X3  X4  X5  X6  X7  X8  | Count the number of mismatches                   | WORK.WANT                */
    /*                                          | in values between two rows,                      |                          */
    /*   1   1   0   1   1   2   .   2   1   1  | going column by column,                          | GRP    ID      FRAC      */
    /*   1   2   1   1   1   2   1   1   1   1  | counting only mismatches                         |                          */
    /*                                          | that don't include an NA.                        |  1      2    0.28571     */
    /*   2   1   2   2   1   0   0   .   0   0  |                                                  |  2      2    0.14286     */
    /*   2   2   2   2   1   1   0   .   0   0  |                                                  |                          */
    /*                                          | COUNT MISSMATCHES BY GROUP IGNORING MISSING      |                          */
    /*                                          | IGNORE X5 INALL CALCULATIONS                     |                          */
    /*                                          |                                                  |                          */
    /*                                          | GRP ID      X1  X2  X3  X4  X5  X6  X7  X8       |                          */
    /*                                          |                                                  |                          */
    /*                                          |   1  1       0   1   1   2   .   2   1   1       |                          */
    /*                                          |   1  2       1   1   1   2   1   1   1   1       |                          */
    /*                                          |                                                  |                          */
    /*                                          |  MIS MATCH   1 + 0 + 0 + 0 + 0 + 1 + 0 + 0   = 2 |                          */
    /*                                          |                                                  |                          */
    /*                                          |  NO MISS     1   1    1  1   0   1   1   1   = 7 |                          */
    /*                                          |                                                  |                          */
    /*                                          |  RESULT =2/7 = 0.2857142857                      |                          */
    /*                                          |                                                  |                          */
    /*------------------------------------------------------------------------------------------------------------------------*/
    /*                                                           |                                                            */
    /*  SAS                                                      |  R                                                         */
    /*  ===                                                      |  =                                                         */
    /*                                                           |                                                            */
    /*                                                           |                                                            */
    /*  data want;                                               |  df %>%                                                    */
    /*                                                           |    pivot_longer(-V1) %>%                                   */
    /*   merge                                                   |    group_by(grp = sub("(.*\\d+).*", "\\1", V1), name) %>%  */
    /*     sd1.have(where=(id=1))                                |    filter(!any(is.na(value))) %>%                          */
    /*     sd1.have(where=(id=2) rename=(x1-x8=y1-y8));          |    summarize(mismatch = value[1] != value[2]) %>%          */
    /*                                                           |    ungroup() %>%                                           */
    /*   by grp;                                                 |    summarize(mismatch =                                    */
    /*                                                           |      paste0(sum(mismatch), "/", n()), .by = grp)           */
    /*   array xs x: ;                                           |                                                            */
    /*   array ys y: ;                                           |                                                            */
    /*                                                           |                                                            */
    /*   notMisCnt=0;                                            |                                                            */
    /*   equCnt=0;                                               |                                                            */
    /*                                                           |                                                            */
    /*   do col=1 to dim(xs);                                    |                                                            */
    /*                                                           |                                                            */
    /*     notMis    = (nmiss(xs[col],ys[col])=0);               |                                                            */
    /*     notMisCnt = notMisCnt + notMis;                       |                                                            */
    /*     equCnt    = equCnt + ((xs[col]=ys[col]) and notMis);  |                                                            */
    /*                                                           |                                                            */
    /*   end;                                                    |                                                            */
    /*                                                           |                                                            */
    /*   NotEquCnt=notMisCnt - EquCnt;                           |                                                            */
    /*   Frac=NotEquCnt/notMisCnt;                               |                                                            */
    /*                                                           |                                                            */
    /*   keep grp id frac                                        |                                                            */
    /*                                                           |                                                            */
    /*                                                           |                                                            */
    /*                                                           |                                                            */
    /*                                                           |                                                            */
    /*                                                           |                                                            */
    /*                                                           |                                                            */
    /*                                                           |                                                            */
    /*                                                           |                                                            */
    /**************************************************************************************************************************/

    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */

    options validvarname=upcase;
    libname sd1 "d:/sd1";
    data sd1.have;
    input grp id x1-x8;
    cards4;
    1 1 0 1 1 2 . 2 1 1
    1 2 1 1 1 2 1 1 1 1
    2 1 2 2 1 0 0 . 0 0
    2 2 2 2 1 1 0 . 0 0
    ;;;;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  Obs    GRP    ID    X1    X2    X3    X4    X5    X6    X7    X8                                                      */
    /*                                                                                                                        */
    /*   1      1      1     0     1     1     2     .     2     1     1                                                      */
    /*   2      1      2     1     1     1     2     1     1     1     1                                                      */
    /*   3      2      1     2     2     1     0     0     .     0     0                                                      */
    /*   4      2      2     2     2     1     1     0     .     0     0                                                      */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*                       _       _            _
    / |  ___  __ _ ___    __| | __ _| |_ __ _ ___| |_ ___ _ __
    | | / __|/ _` / __|  / _` |/ _` | __/ _` / __| __/ _ \ `_ \
    | | \__ \ (_| \__ \ | (_| | (_| | || (_| \__ \ ||  __/ |_) |
    |_| |___/\__,_|___/  \__,_|\__,_|\__\__,_|___/\__\___| .__/
                                                         |_|
    */

    data want;

     merge
       sd1.have(where=(id=1))
       sd1.have(where=(id=2) rename=(x1-x8=y1-y8));

     by grp;

     array xs x: ;
     array ys y: ;

     notMisCnt=0;
     equCnt=0;

     do col=1 to dim(xs);

       notMis    = (nmiss(xs[col],ys[col])=0);
       notMisCnt = notMisCnt + notMis;
       equCnt    = equCnt + ((xs[col]=ys[col]) and notMis);

     end;

     NotEquCnt=notMisCnt - EquCnt;
     Frac=NotEquCnt/notMisCnt;
     keep grp id frac;

    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* WORK.WANT total obs=2                                                                                                  */
    /*                                                                                                                        */
    /*  GRP    ID      FRAC                                                                                                   */
    /*                                                                                                                        */
    /*   1      2    0.28571                                                                                                  */
    /*   2      2    0.14286                                                                                                  */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*___                              _             __
    |___ \   ___  __ _ ___   ___  __ _| | __      __/ /__    __ _ _ __ _ __ __ _ _   _ ___
      __) | / __|/ _` / __| / __|/ _` | | \ \ /\ / / / _ \  / _` | `__| `__/ _` | | | / __|
     / __/  \__ \ (_| \__ \ \__ \ (_| | |  \ V  V / / (_) || (_| | |  | | | (_| | |_| \__ \
    |_____| |___/\__,_|___/ |___/\__, |_|   \_/\_/_/ \___/  \__,_|_|  |_|  \__,_|\__, |___/
                                    |_|                                          |___/
     _ __   ___     __ _ _ __ _ __ __ _ _   _ ___
    | `_ \ / _ \   / _` | `__| `__/ _` | | | / __|
    | | | | (_) | | (_| | |  | | | (_| | |_| \__ \
    |_| |_|\___/   \__,_|_|  |_|  \__,_|\__, |___/
                                        |___/
    */

    proc sql;
      create
         table want as
      select
         l.grp
        ,sum(
           (l.x1 ne  r.x1) * (nmiss(l.x1, r.x1)=0),
           (l.x2 ne  r.x2) * (nmiss(l.x2, r.x2)=0),
           (l.x3 ne  r.x3) * (nmiss(l.x3, r.x3)=0),
           (l.x4 ne  r.x4) * (nmiss(l.x4, r.x4)=0),
           (l.x5 ne  r.x5) * (nmiss(l.x5, r.x5)=0),
           (l.x6 ne  r.x6) * (nmiss(l.x6, r.x6)=0),
           (l.x7 ne  r.x7) * (nmiss(l.x7, r.x7)=0),
           (l.x8 ne  r.x8) * (nmiss(l.x8, r.x8)=0)
         ) /
         sum(
           (nmiss(l.x1, r.x1)=0),
           (nmiss(l.x2, r.x2)=0),
           (nmiss(l.x3, r.x3)=0),
           (nmiss(l.x4, r.x4)=0),
           (nmiss(l.x5, r.x5)=0),
           (nmiss(l.x6, r.x6)=0),
           (nmiss(l.x7, r.x7)=0),
           (nmiss(l.x8, r.x8)=0)
          )  as frac
      from
        sd1.have as l, sd1.have as r
      where
            l.grp = r.grp
        and l.id =  r.id-1
      order
        by l.grp

    ;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* WORK.WANT total obs=2                                                                                                  */
    /*                                                                                                                        */
    /* Obs    GRP      FRAC                                                                                                   */
    /*                                                                                                                        */
    /*  1      1     0.28571                                                                                                  */
    /*  2      2     0.14286                                                                                                  */
    /*                                                                                                                        */
    /**************************************************************************************************************************/
    /*
      __ _ _ __ _ __ __ _ _   _ ___
     / _` | `__| `__/ _` | | | / __|
    | (_| | |  | | | (_| | |_| \__ \
     \__,_|_|  |_|  \__,_|\__, |___/
                          |___/
    */

    proc datasets lib=work nodetails nolist;
     delete want;
    run;quit;

    %array(_vs,values=1-8);

    proc sql;
      create
         table want as
      select
         l.grp
        ,sum(
           %do_over(_vs,phrase=%str((l.x? ne r.x?) * (nmiss(l.x?, r.x?)=0)),between=comma)
              ) /
         sum(
           %do_over(_vs,phrase=%str((nmiss(l.x?, r.x?)=0)),between=comma)
         )
      from
        sd1.have as l, sd1.have as r
      where
            l.grp = r.grp
        and l.id =  r.id-1
      order
        by l.grp

    ;quit;

    %arraydelete(_vs)


    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* WORK.WANT total obs=2                                                                                                  */
    /*                                                                                                                        */
    /* Obs    GRP      FRAC                                                                                                   */
    /*                                                                                                                        */
    /*  1      1     0.28571                                                                                                  */
    /*  2      2     0.14286                                                                                                  */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*____                    _             __
    |___ /   _ __   ___  __ _| | __      __/ /__     __ _ _ __ _ __ __ _ _   _ ___
      |_ \  | `__| / __|/ _` | | \ \ /\ / / / _ \   / _` | `__| `__/ _` | | | / __|
     ___) | | |    \__ \ (_| | |  \ V  V / / (_) | | (_| | |  | | | (_| | |_| \__ \
    |____/  |_|    |___/\__, |_|   \_/\_/_/ \___/   \__,_|_|  |_|  \__,_|\__, |___/
                           |_|                                           |___/
     _ __   ___     __ _ _ __ _ __ __ _ _   _ ___
    | `_ \ / _ \   / _` | `__| `__/ _` | | | / __|
    | | | | (_) | | (_| | |  | | | (_| | |_| \__ \
    |_| |_|\___/   \__,_|_|  |_|  \__,_|\__, |___/
    */

    %utl_submit_r64x('
    library(sqldf);
    library(haven);
    source("c:/oto/fn_tosas9x.R");
    have<-read_sas("d:/sd1/have.sas7bdat");
    want<-sqldf("
      select
        (
         0.0 +
         ((l.X1 !=  r.X1) and not (l.X1 is null or r.X1 is null)) +
         ((l.X2 !=  r.X2) and not (l.X2 is null or r.X2 is null)) +
         ((l.X3 !=  r.X3) and not (l.X3 is null or r.X3 is null)) +
         ((l.X4 !=  r.X4) and not (l.X4 is null or r.X4 is null)) +
         ((l.X5 !=  r.X5) and not (l.X5 is null or r.X5 is null)) +
         ((l.X6 !=  r.X6) and not (l.X6 is null or r.X6 is null)) +
         ((l.X7 !=  r.X7) and not (l.X7 is null or r.X7 is null)) +
         ((l.X8 !=  r.X8) and not (l.X8 is null or r.X8 is null))
        ) /
        (
         0.0 +
         (not (l.X1 is null or r.X1 is null)) +
         (not (l.X2 is null or r.X2 is null)) +
         (not (l.X3 is null or r.X3 is null)) +
         (not (l.X4 is null or r.X4 is null)) +
         (not (l.X5 is null or r.X5 is null)) +
         (not (l.X6 is null or r.X6 is null)) +
         (not (l.X7 is null or r.X7 is null)) +
         (not (l.X8 is null or r.X8 is null))
        )  as frac

      from
         have as l, have as r
      where
            l.grp = r.grp
        and l.id =  r.id-1
      order
        by l.grp
      ");
    want
    ');

    /*
      __ _ _ __ _ __ __ _ _   _ ___
     / _` | `__| `__/ _` | | | / __|
    | (_| | |  | | | (_| | |_| \__ \
     \__,_|_|  |_|  \__,_|\__, |___/
                          |___/
    */

    %utl_submit_r64x("
    library(sqldf);
    library(haven);
    source('c:/oto/fn_tosas9x.R');
    have<-read_sas('d:/sd1/have.sas7bdat');
    want<-sqldf('
      select
        l.grp
        ,(
         0.0 +
         %do_over(_vs,phrase=%str(((l.X? !=  r.X?) and not (l.X? is null or r.X? is null))),between=+)
        ) /
        (
         0.0 +
         %do_over(_vs,phrase=%str((not (l.X? is null or r.X? is null))),between=+)
        ) as frac
      from
         have as l, have as r
      where
            l.grp = r.grp
        and l.id =  r.id-1
      order
        by l.grp
      ');
    want;
    fn_tosas9x(
          inp    = want
         ,outlib ='d:/sd1/'
         ,outdsn ='rwant'
         );
    ");

    libname sd1 "d:/sd1";
    proc print data=sd1.rwant;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*   ROWNAMES    GRP      FRAC                                                                                            */
    /*                                                                                                                        */
    /*       1        1     0.28571                                                                                           */
    /*       2        2     0.14286                                                                                           */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*  _                 _   _                             _
    | || |    _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
    | || |_  | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
    |__   _| | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
       |_|   | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
             |_|    |___/                                |_|
    */


    %utl_pybeginx;
    parmcards4;
    import pyarrow.feather as feather;
    import tempfile;
    import pyperclip;
    import os;
    import sys;
    import subprocess;
    import time;
    import pandas as pd;
    import pyreadstat as ps;
    import numpy as np;
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    sqlite3conn = next(pdsql.conn.gen).connection.connection;
    sqlite3conn.enable_load_extension(True);
    sqlite3conn.load_extension("c:/temp/libsqlitefunctions.dll");
    mysql = lambda q: sqldf(q, globals());
    exec(open("c:/oto/fn_tosas9x.py").read());
    have, meta = ps.read_sas7bdat("d:/sd1/have.sas7bdat");
    want=pdsql("""
      select
        (
         0.0 +
         ((l.X1 !=  r.X1) and not (l.X1 is null or r.X1 is null)) +
         ((l.X2 !=  r.X2) and not (l.X2 is null or r.X2 is null)) +
         ((l.X3 !=  r.X3) and not (l.X3 is null or r.X3 is null)) +
         ((l.X4 !=  r.X4) and not (l.X4 is null or r.X4 is null)) +
         ((l.X5 !=  r.X5) and not (l.X5 is null or r.X5 is null)) +
         ((l.X6 !=  r.X6) and not (l.X6 is null or r.X6 is null)) +
         ((l.X7 !=  r.X7) and not (l.X7 is null or r.X7 is null)) +
         ((l.X8 !=  r.X8) and not (l.X8 is null or r.X8 is null))
        ) /
        (
         0.0 +
         (not (l.X1 is null or r.X1 is null)) +
         (not (l.X2 is null or r.X2 is null)) +
         (not (l.X3 is null or r.X3 is null)) +
         (not (l.X4 is null or r.X4 is null)) +
         (not (l.X5 is null or r.X5 is null)) +
         (not (l.X6 is null or r.X6 is null)) +
         (not (l.X7 is null or r.X7 is null)) +
         (not (l.X8 is null or r.X8 is null))
        )  as frac

      from
         have as l, have as r
      where
            l.grp = r.grp
        and l.id =  r.id-1
      order
        by l.grp
       """)
    print(want);
    fn_tosas9x(want,outlib="d:/sd1/",outdsn="rwant",timeest=3);
    ;;;;
    %utl_pyendx;

    libname sd1 "d:/sd1";
    proc print data=sd1.rwant;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  R                                                                                                                     */
    /*                                                                                                                        */
    /*         frac                                                                                                           */
    /*  0  0.285714                                                                                                           */
    /*  1  0.142857                                                                                                           */
    /*                                                                                                                        */
    /*  SAS                                                                                                                   */
    /*                                                                                                                        */
    /*  Obs      FRAC                                                                                                         */
    /*                                                                                                                        */
    /*   1     0.28571                                                                                                        */
    /*   2     0.14286                                                                                                        */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    %array(_vs,values=1-8);

    %utl_submit_py64_310x("
    import pyarrow.feather as feather;
    import tempfile;
    import pyperclip;
    import os;
    import sys;
    import subprocess;
    import time;
    import pandas as pd;
    import pyreadstat as ps;
    import numpy as np;
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    sqlite3conn = next(pdsql.conn.gen).connection.connection;
    sqlite3conn.enable_load_extension(True);
    sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
    mysql = lambda q: sqldf(q, globals());
    exec(open('c:/oto/fn_tosas9x.py').read());
    have, meta = ps.read_sas7bdat('d:/sd1/have.sas7bdat');
    print(have);
    want=pdsql('''
      select
        l.grp
        ,(
         0.0 +
         %do_over(_vs,phrase=%str(((l.X? !=  r.X?) and not (l.X? is null or r.X? is null))),between=+)
        ) /
        (
         0.0 +
         %do_over(_vs,phrase=%str((not (l.X? is null or r.X? is null))),between=+)
        ) as frac
      from
         have as l, have as r
      where
            l.grp = r.grp
        and l.id =  r.id-1
      order
        by l.grp
       ''');
    print(want);
    fn_tosas9x(want,outlib='d:/sd1/',outdsn='pywant',timeest=3);
    ");

    proc print data=sd1.pywant;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* python                                                                                                                 */
    /*                                                                                                                        */
    /*    GRP      frac                                                                                                       */
    /* 0  1.0  0.285714                                                                                                       */
    /* 1  2.0  0.142857                                                                                                       */
    /*                                                                                                                        */
    /* SAS                                                                                                                    */
    /*                                                                                                                        */
    /* Obs    GRP      FRAC                                                                                                   */
    /*                                                                                                                        */
    /*  1      1     0.28571                                                                                                  */
    /*  2      2     0.14286                                                                                                  */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
