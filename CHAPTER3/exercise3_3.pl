#!/usr/bin/perl

print ("Enter number 47\n");

$num = <STDIN>;

until ($num == 47) {

	print ("Keep trying!\n");

	$num = <STDIN>;

}

print ("Correct!",\b,"\n");
