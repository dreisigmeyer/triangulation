#!/usr/bin/perl

use strict;
use warnings;

open (A1FILE, "<", "a1_sorted_grouped_counted.csv")
    or die "Can't open a1_sorted_grouped_counted.csv: $!";
open (A2FILE, "<", "a2_sorted_grouped_counted.csv")
    or die "Can't open a2_sorted_grouped_counted.csv: $!";
open (A3FILE, "<", "a3_sorted_grouped_counted.csv")
    or die "Can't open a3_sorted_grouped_counted.csv: $!";

open (my $a1HoldFile, ">", "a1_hold.csv")
    or die "Can't open a1_hold.csv: $!";
open (my $a2HoldFile, ">", "a2_hold.csv")
    or die "Can't open a2_hold.csv: $!";
open (my $a3HoldFile, ">", "a3_hold.csv")
    or die "Can't open a3_hold.csv: $!";    
    
open (my $a1OutFile, ">", "a1_final.csv")
    or die "Can't open a1_final.csv: $!";
open (my $a2OutFile, ">", "a2_final.csv")
    or die "Can't open a2_final.csv: $!";
open (my $a3OutFile, ">", "a3_final.csv")
    or die "Can't open a3_final.csv: $!";
    
my $prdn = '';
# These will always be less/greater than allowable values:
my $assSeq = 0;
my $invOffsetYr = 100;
my $assOffsetYr = 100;
my $uniqCount = 0;
my $empYrHold = 5000;
my $crosswalkYrHold = 5000;
while (my $line = <A1FILE>) {
    my ($count,$prdnHold,$assSeqHold,$absGrantYr,$crosswalkYr,$absAppYr,$empYr,$assFirmid,$grant_yr,$rest) = (split /,/, $line, 10);
    if ($prdnHold ne $prdn) { # This is the first line of a new patent and must be a 'keeper'
        $prdn = $prdnHold;
        $assSeq = $assSeqHold;
        $invOffsetYr = $absAppYr;
        $empYrHold = $empYr;
        $assOffsetYr = $absGrantYr;
        $crosswalkYrHold = $crosswalkYr;
        $uniqCount = $count;
        print $a1HoldFile $line;
        next;
    }
    if ($assSeqHold > $assSeq) { # This is the first line of a new assignee and must be a 'keeper'
        $assSeq = $assSeqHold;
        $invOffsetYr = $absAppYr;
        $empYrHold = $empYr;
        $assOffsetYr = $absGrantYr;
        $crosswalkYrHold = $crosswalkYr;
        $uniqCount = $count;
        print $a1HoldFile $line;
        next;
    }
    # These are ordered by increasing year within any $absGrantYr/$absAppYr in the input files:    
    ($absAppYr <= $invOffsetYr) ? $invOffsetYr = $absAppYr : next;
    ($absGrantYr <= $assOffsetYr) ? $assOffsetYr = $absGrantYr : next;
    ($empYr <= $empYrHold) ?  $empYrHold = $empYr : next;
    ($crosswalkYr <= $crosswalkYrHold) ?  $crosswalkYrHold = $crosswalkYr : next;
    # This is ordered by decreasing $count in the input files:
    ($count >= $uniqCount) ? $uniqCount = $count : next;
    print $a1HoldFile $line;
}
close(A1FILE);
close($a1HoldFile);

$prdn = '';
$assSeq = 0;
$invOffsetYr = 100;
$assOffsetYr = 100;
$uniqCount = 0;
$empYrHold = 5000;
$crosswalkYrHold = 5000;
while (my $line = <A2FILE>) {
    my ($count,$prdnHold,$assSeqHold,$absGrantYr,$crosswalkYr,$absAppYr,$empYr,$assFirmid,$grant_yr,$rest) = (split /,/, $line, 10);
    if ($prdnHold ne $prdn) {
        $prdn = $prdnHold;
        $assSeq = $assSeqHold;
        $invOffsetYr = $absAppYr;
        $empYrHold = $empYr;
        $assOffsetYr = $absGrantYr;
        $crosswalkYrHold = $crosswalkYr;
        $uniqCount = $count;
        print $a2HoldFile $line;
        next;
    }
    if ($assSeqHold > $assSeq) {
        $assSeq = $assSeqHold;
        $invOffsetYr = $absAppYr;
        $empYrHold = $empYr;
        $assOffsetYr = $absGrantYr;
        $crosswalkYrHold = $crosswalkYr;
        $uniqCount = $count;
        print $a2HoldFile $line;
        next;
    }
    ($absAppYr <= $invOffsetYr) ? $invOffsetYr = $absAppYr : next;
    ($absGrantYr <= $assOffsetYr) ? $assOffsetYr = $absGrantYr : next;
    ($empYr <= $empYrHold) ?  $empYrHold = $empYr : next;
    ($crosswalkYr <= $crosswalkYrHold) ?  $crosswalkYrHold = $crosswalkYr : next;
    ($count >= $uniqCount) ? $uniqCount = $count : next;
    print $a2HoldFile $line;
}
close(A2FILE);
close($a2HoldFile);

$prdn = '';
$assSeq = 0;
$invOffsetYr = 100;
$assOffsetYr = 100;
$uniqCount = 0;
$empYrHold = 5000;
$crosswalkYrHold = 5000;
while (my $line = <A3FILE>) {
    my ($count,$prdnHold,$assSeqHold,$absGrantYr,$crosswalkYr,$absAppYr,$empYr,$assFirmid,$grant_yr,$rest) = (split /,/, $line, 10);
    if ($prdnHold ne $prdn) {
        $prdn = $prdnHold;
        $assSeq = $assSeqHold;
        $invOffsetYr = $absAppYr;
        $empYrHold = $empYr;
        $assOffsetYr = $absGrantYr;
        $crosswalkYrHold = $crosswalkYr;
        $uniqCount = $count;
        print $a3HoldFile $line;
        next;
    }
    if ($assSeqHold > $assSeq) {
        $assSeq = $assSeqHold;
        $invOffsetYr = $absAppYr;
        $empYrHold = $empYr;
        $assOffsetYr = $absGrantYr;
        $crosswalkYrHold = $crosswalkYr;
        $uniqCount = $count;
        print $a3HoldFile $line;
        next;
    }
    ($absAppYr <= $invOffsetYr) ? $invOffsetYr = $absAppYr : next;
    ($absGrantYr <= $assOffsetYr) ? $assOffsetYr = $absGrantYr : next;
    ($empYr <= $empYrHold) ?  $empYrHold = $empYr : next;
    ($crosswalkYr <= $crosswalkYrHold) ?  $crosswalkYrHold = $crosswalkYr : next;
    ($count >= $uniqCount) ? $uniqCount = $count : next;
    print $a3HoldFile $line;
}
close(A3FILE);
close($a3HoldFile);

# This was all thrown on at the end
my %multiples;
my @check_uniq = `cut -d',' -f2,3,8 a*_hold.csv | sort -u | cut -d',' -f1,2 | uniq -c | sed -e 's/^[ ]*\\([0-9]*\\) /\\1,/' | awk -F',' '{if (\$1 > 1) {print \$2,\$3}}'`;

foreach my $line (@check_uniq) {
    chomp $line;
    my ($prdn,$assSeq) = (split / /, $line);
    $multiples{$prdn}{$assSeq} = 1;
}

open (A1FILE, "<", "a1_hold.csv")
    or die "Can't open a1_hold.csv: $!";
open (A2FILE, "<", "a2_hold.csv")
    or die "Can't open a2_hold.csv: $!";
open (A3FILE, "<", "a3_hold.csv")
    or die "Can't open a3_hold.csv: $!";

while (my $line = <A1FILE>) {
    chomp $line;
    my ($prdn,$assSeq) = (split /,/, $line)[1,2];
    if (exists $multiples{$prdn}{$assSeq}) {
        print $a1OutFile ($line . ",A1,1\n");
    } else {
        print $a1OutFile ($line . ",A1,0\n");
    }
}
while (my $line = <A2FILE>) {
    chomp $line;
    my ($prdn,$assSeq) = (split /,/, $line)[1,2];
    if (exists $multiples{$prdn}{$assSeq}) {
        print $a2OutFile ($line . ",A2,1\n");
    } else {
        print $a2OutFile ($line . ",A2,0\n");
    }
}
while (my $line = <A3FILE>) {
    chomp $line;
    my ($prdn,$assSeq) = (split /,/, $line)[1,2];
    if (exists $multiples{$prdn}{$assSeq}) {
        print $a3OutFile ($line . ",A3,1\n");
    } else {
        print $a3OutFile ($line . ",A3,0\n");
    }
}

close(A1FILE);
close(A2FILE);
close(A3FILE);
close($a1OutFile);
close($a2OutFile);
close($a3OutFile);
