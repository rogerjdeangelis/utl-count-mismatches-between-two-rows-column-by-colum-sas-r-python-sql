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
