/* combined.sas = autoexec.sas + script.sas, self-contained for a single POST. */
/* The runner submits this same concatenation; script.sas holds your DATA step verbatim. */

/* cap input rows for the captured run */
options obs=100;
options validvarname=upcase;

/* SOL1 uses libname sd1; point it at a writable working directory */
libname sd1 ".";

/* seed sd1.have from the author's cards4 block (verbatim values) */
data sd1.have;
input grp id x1-x8;
cards4;
1 1 0 1 1 2 . 2 1 1
1 2 1 1 1 2 1 1 1 1
2 1 2 2 1 0 0 . 0 0
2 2 2 2 1 1 0 . 0 0
;;;;
run;quit;

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
