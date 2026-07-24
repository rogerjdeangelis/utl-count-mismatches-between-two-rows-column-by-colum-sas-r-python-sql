/* Solution 1: SAS DATA step -- count mismatches between two rows,     */
/* column by column, ignoring pairs that include a missing value.      */
/* Verbatim from utl-count-mismatches...sas (data want; ...).          */

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

proc print data=want;
run;quit;
