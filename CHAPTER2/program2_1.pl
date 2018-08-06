#!/usr/bin/perl
print ("Enter the distance to be converted:\n");
$originaldist = <STDIN>;
chop ($originaldist);
$miles = $originaldist * 0.6214;
$kilometers = $originaldist * 1.609;
print ($originaldist, " kilometers = ", $miles,
       " miles\n");
print ($originaldist, " miles = ", $kilometers,
       " kilometers\n");
