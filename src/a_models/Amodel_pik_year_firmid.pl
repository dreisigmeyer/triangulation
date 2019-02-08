#!/usr/bin/perl

use strict;
use warnings;


open (A1FILE, "<", "closed_paths_A1.csv")
    or die "Can't open closed_paths_A1.csv: $!";
open (A2FILE, "<", "closed_paths_A2.csv")
    or die "Can't open closed_paths_A2.csv: $!";
open (A3FILE, "<", "closed_paths_A3.csv")
    or die "Can't open closed_paths_A3.csv: $!";

open (LOOPS, "<", "A_info_hold.csv")
    or die "Can't open A_info_hold.csv: $!";
    
open (my $outFile, ">", "Amodel_pik_year_firmid.csv")
    or die "Can't open Amodel_pik_year_firmid.csv: $!";

# This gives us the PIKs that will be associated with every closed loop
my %sqlData;

while (my $line = <A1FILE>) {
    my ($prdn,$assSeq,$pik,$cwYr,$empYr,$firmid) = (split /,/, $line)[1,4,6,7,8,12];
    $sqlData{$prdn}{$assSeq}{$cwYr}{$empYr}{$firmid}{$pik} = 1;   
}
close(A1FILE);

while (my $line = <A2FILE>) {
    my ($prdn,$assSeq,$pik,$cwYr,$empYr,$firmid) = (split /,/, $line)[1,4,6,7,8,12];
    $sqlData{$prdn}{$assSeq}{$cwYr}{$empYr}{$firmid}{$pik} = 1;
}
close(A2FILE);

while (my $line = <A3FILE>) {
    my ($prdn,$assSeq,$pik,$cwYr,$empYr,$firmid) = (split /,/, $line)[1,4,6,7,8,12];
    $sqlData{$prdn}{$assSeq}{$cwYr}{$empYr}{$firmid}{$pik} = 1;
}
close(A3FILE);

while (my $line = <LOOPS>) {
    chomp $line;
    my ($prdn,$assSeq,$cwYr,$empYr,$firmid) = (split /,/, $line);  
    foreach my $pik ( keys %{ $sqlData{"$prdn"}{"$assSeq"}{"$cwYr"}{"$empYr"}{"$firmid"} }) {
        print $outFile "$pik,$empYr,$firmid\n";
    }
}
close(LOOPS);
close($outFile);
