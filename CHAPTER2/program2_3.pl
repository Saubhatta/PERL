#!/usr/bin/perl

print ("Enter a number:\n");

$number1 = <STDIN>;

chop ($number1);

print ("Enter another number:\n");

$number2 = <STDIN>;

chop ($number2);

if ($number1 == $number2) {

		print ("Two numbers are same/equal.\n");
}

print ("This is last line of the program.\n");
