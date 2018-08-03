#!/usr/bin/perl

print ("Enter the number to multiply:\n");

$number_input = <STDIN>;

chop ($number_input);

$numer_output = $number_input * 2;	#Multiplies by 2

print ($number_input, " * 2 equals ", $numer_output, "\n");
